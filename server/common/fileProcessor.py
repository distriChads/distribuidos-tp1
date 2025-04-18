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

MAX_BATCH_SIZE = 8000 - 4  # 4 bytes for the file size


class Processor:
    def __init__(self):
        self.header_length = 0
        self.fields_count = 0
        self.data_buffer: str = ""
        self.overflow_buffer: str = ""

    def process_first_batch(self, chunck_received: str) -> int:
        index_delimiter = chunck_received.find('|')

        file_size = int(chunck_received[:index_delimiter])
        print(f"File size: {chunck_received[:index_delimiter+3]}")
        print(f"File size: {file_size}")
        chunck_received = chunck_received[index_delimiter+1:]

        chunck_received = self.remove_header(chunck_received)
        self.process_batch(chunck_received)

        return file_size

    def process_batch(self, csv_data: str):
        self.data_buffer += self.overflow_buffer
        self.overflow_buffer = ""
        successful_lines_count = 0
        error_count = 0
        reader = csv.reader(io.StringIO(csv_data))

        for row in reader:
            try:
                if len(row) == 0 or len(row) != self.fields_count:
                    error_count += 1
                    logging.error(
                        f"Error processing line, Expected {self.fields_count} fields, got {len(row)}")
                    continue
                line_processed = self._process_line(row)
                successful_lines_count += 1
                if len(self.data_buffer) + len(line_processed) + 1 <= MAX_BATCH_SIZE:
                    self.data_buffer += line_processed + "\n"
                else:
                    self.overflow_buffer += line_processed + "\n"
            except Exception as e:
                error_count += 1
                logging.error(f"Error processing line, Error: {e}")
                continue
        logging.info(
            f"Processed {successful_lines_count} lines, {error_count} errors")

    def remove_header(self, csv_data: str) -> str:
        return csv_data[self.header_length:]

    def ready_to_send(self) -> bool:
        # -1 for the \n
        return len(self.data_buffer) + len(self.overflow_buffer) - 1 >= MAX_BATCH_SIZE

    def get_processed_batch(self) -> str:
        result = self.data_buffer
        self.data_buffer = ""
        return result

    def get_all_data(self) -> str:
        result = self.data_buffer + self.overflow_buffer
        self.data_buffer = ""
        self.overflow_buffer = ""
        return result

    def _process_line(self, line: list[str]) -> str:
        raise NotImplementedError(
            "Subclasses should implement this method")

    def _try_parse_python_structure(self, text: str):
        try:
            return ast.literal_eval(text)
        except (ValueError, SyntaxError):
            return ""


class MoviesProcessor(Processor):
    def __init__(self):
        super().__init__()
        self.header_length = len(MOVIES_HEADER) + 1  # +1 for the \n
        self.fields_count = FIELDS_COUNT_MOVIES

    def _process_line(self, line: list[str]) -> str:
        id = line[5]
        title = line[20]
        releaseDate = line[14]
        prodCountries = line[13]
        genres = line[3]

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

        countries = VALUE_SEPARATOR.join(
            [c["iso_3166_1"] for c in prodCountries])
        genres = VALUE_SEPARATOR.join([g["name"] for g in genres])

        return f"{id}{FIELD_SEPARATOR}{title}{FIELD_SEPARATOR}{releaseDate}{FIELD_SEPARATOR}{countries}{FIELD_SEPARATOR}{genres}"


class CreditsProcessor(Processor):
    def __init__(self):
        super().__init__()
        self.header_length = len(CREDITS_HEADER) + 1  # +1 for the \n
        self.fields_count = FIELDS_COUNT_CREDITS

    def _process_line(self, line: list[str]) -> str:
        id = line[2]
        cast = line[0]

        cast = self._try_parse_python_structure(cast)
        if not cast:
            raise EmptyFieldError("Missing cast")
        if not id:
            raise EmptyFieldError("Missing id")

        cast = VALUE_SEPARATOR.join([c["name"] for c in cast])
        return f"{id}{FIELD_SEPARATOR}{cast}"


class RatingsProcessor(Processor):
    def __init__(self):
        super().__init__()
        self.header_length = len(RATINGS_HEADER) + 1  # +1 for the \n
        self.fields_count = FIELDS_COUNT_RATINGS

    def _process_line(self, line: list[str]) -> str:
        movieId = line[1]
        rating = line[2]

        if not movieId:
            raise EmptyFieldError("Missing movieId")
        if not rating:
            raise EmptyFieldError("Missing rating")

        return f"{movieId}{FIELD_SEPARATOR}{rating}"


class EmptyFieldError(Exception):
    pass
