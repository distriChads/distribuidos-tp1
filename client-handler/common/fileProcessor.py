import ast
import csv
import io
import logging


MOVIES_HEADER = "adult,belongs_to_collection,budget,genres,homepage,id,imdb_id,original_language,original_title,overview,popularity,poster_path,production_companies,production_countries,release_date,revenue,runtime,spoken_languages,status,tagline,title,video,vote_average,vote_count"
CREDITS_HEADER = "cast,crew,id"
RATINGS_HEADER = "userId,movieId,rating,timestamp"

FIELDS_COUNT_MOVIES = 24
FIELDS_COUNT_CREDITS = 3
FIELDS_COUNT_RATINGS = 4

FIELD_SEPARATOR = "|"
VALUE_SEPARATOR = ","

MAX_BATCH_SIZE = (1024 * 8) - 4  # 4 bytes for the file size


class Processor:
    def __init__(self, routing_keys_count: int):
        self.header_length = 0
        self.fields_count = 0
        self.data_buffer: list[list[str]] = [[]
                                             for _ in range(routing_keys_count)]

        self.bytes_read = 0
        self.read_until = 0
        self.errors_per_file = 0
        self.successful_lines_count = 0

    def process_first_batch(self, bytes_received: int, chunck_received: str):
        index_delimiter = chunck_received.find('|')

        file_size = int(chunck_received[:index_delimiter])
        self.read_until = file_size
        chunck_received = chunck_received[index_delimiter+1:]

        chunck_received = self.remove_header(chunck_received)
        self.process_batch(bytes_received, chunck_received)

    def process_batch(self, bytes_received: int, chunck_received: str):
        self.bytes_read += bytes_received
        successful_lines_count = 0
        error_count = 0
        reader = csv.reader(io.StringIO(chunck_received))

        for row in reader:
            try:
                if len(row) == 0 or len(row) != self.fields_count:
                    error_count += 1
                    logging.debug(
                        f"Error processing line, Expected {self.fields_count} fields, got {len(row)}")
                    continue

                hash_idx, line_processed = self._process_line(row)
                hash = int(hash_idx) % len(self.data_buffer)
                self.data_buffer[hash].append(line_processed + "\n")
                successful_lines_count += 1

            except Exception as e:
                error_count += 1
                logging.debug(f"Error processing line, Error: {e}")
                continue

        self.successful_lines_count += successful_lines_count
        self.errors_per_file += error_count

    def received_all_data(self) -> bool:
        return self.bytes_read >= self.read_until

    def remove_header(self, csv_data: str) -> str:
        return csv_data[self.header_length:]

    def get_processed_batch(self) -> list[list[str]]:
        results: list[str] = ["" for _ in range(len(self.data_buffer))]
        for i in range(len(self.data_buffer)):
            if len(self.data_buffer[i]) > 0:
                results[i] = "".join(self.data_buffer[i])
                self.data_buffer[i].clear()
        return results

    def _process_line(self, line: list[str]) -> tuple[str, str]:
        raise NotImplementedError(
            "Subclasses should implement this method")

    def _try_parse_python_structure(self, text: str):
        try:
            return ast.literal_eval(text)
        except (ValueError, SyntaxError):
            return ""


class MoviesProcessor(Processor):
    def __init__(self, routing_keys_count: int):
        super().__init__(routing_keys_count)
        self.header_length = len(MOVIES_HEADER) + 1  # +1 for the \n
        self.fields_count = FIELDS_COUNT_MOVIES

    def _process_line(self, line: list[str]) -> tuple[str, str]:
        budget = line[2]
        genres = line[3]
        id = line[5]
        overview = line[9]
        prodCountries = line[13]
        releaseDate = line[14]
        revenue = line[15]
        spokenLanguages = line[17]
        title = line[20]

        if "\n" in overview:
            overview = overview.split("\n")
            overview = " ".join(overview)

        prodCountries = self._try_parse_python_structure(prodCountries)
        genres = self._try_parse_python_structure(genres)

        if not id:
            raise EmptyFieldError("Missing id")
        if not title:
            raise EmptyFieldError("Missing title")
        if not releaseDate:
            raise EmptyFieldError("Missing release date")
        if not prodCountries:
            raise EmptyFieldError("Missing production countries")
        if not genres:
            raise EmptyFieldError("Missing genres")
        if not budget:
            raise EmptyFieldError("Missing budget")
        if not spokenLanguages:
            raise EmptyFieldError("Missing spoken languages")
        if not revenue:
            raise EmptyFieldError("Missing revenue")
        if not overview:
            raise EmptyFieldError("Missing overview")

        countries = VALUE_SEPARATOR.join(
            [c["iso_3166_1"] for c in prodCountries])
        genres = VALUE_SEPARATOR.join([g["name"] for g in genres])

        return id, f"{id}{FIELD_SEPARATOR}{title}{FIELD_SEPARATOR}{releaseDate}{FIELD_SEPARATOR}{countries}{FIELD_SEPARATOR}{genres}{FIELD_SEPARATOR}{budget}{FIELD_SEPARATOR}{overview}{FIELD_SEPARATOR}{revenue}"


class CreditsProcessor(Processor):
    def __init__(self, routing_keys_count: int):
        super().__init__(routing_keys_count)
        self.header_length = len(CREDITS_HEADER) + 1  # +1 for the \n
        self.fields_count = FIELDS_COUNT_CREDITS
        self.row_length = 0
        self.row_buffer = ""

    def process_batch(self, bytes_received: int, chunck_received: str):
        if len(chunck_received) == 0:
            self.bytes_read += bytes_received
            # was the header - skip it
            return
        if self.row_length == 0:
            index_delimiter = chunck_received.find("|")
            str_length = chunck_received[:index_delimiter]
            self.row_length = int(str_length)
            chunck_received = chunck_received[index_delimiter+1:]
            # 4 bytes for the row size + 1 byte for the delimiter
            bytes_received -= len(str_length) + 1
        self.row_buffer += chunck_received
        if len(self.row_buffer) == self.row_length:
            super().process_batch(bytes_received, self.row_buffer)
            self.row_buffer = ""
            self.row_length = 0
        else:
            self.bytes_read += bytes_received

    def _process_line(self, line: list[str]) -> tuple[str, str]:
        cast = line[0]
        id = line[2]

        cast = self._try_parse_python_structure(cast)
        if not cast:
            raise EmptyFieldError("Missing cast")
        if not id:
            raise EmptyFieldError("Missing id")

        cast = VALUE_SEPARATOR.join([c["name"] for c in cast])
        return id, f"{id}{FIELD_SEPARATOR}{cast}"


class RatingsProcessor(Processor):
    def __init__(self, routing_keys_count: int):
        super().__init__(routing_keys_count)
        self.header_length = len(RATINGS_HEADER) + 1  # +1 for the \n
        self.fields_count = FIELDS_COUNT_RATINGS

    def _process_line(self, line: list[str]) -> tuple[str, str]:
        movieId = line[1]
        rating = line[2]
        timestamp = line[3]

        if not movieId:
            raise EmptyFieldError("Missing movieId")
        if not rating:
            raise EmptyFieldError("Missing rating")
        if not timestamp:
            raise EmptyFieldError("Missing timestamp")

        return movieId, f"{movieId}{FIELD_SEPARATOR}{rating}"


class EmptyFieldError(Exception):
    pass
