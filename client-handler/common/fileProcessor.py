import ast
import csv
import io
import logging

from .hasher import HasherContainer


MOVIES_HEADER = "adult,belongs_to_collection,budget,genres,homepage,id,imdb_id,original_language,original_title,overview,popularity,poster_path,production_companies,production_countries,release_date,revenue,runtime,spoken_languages,status,tagline,title,video,vote_average,vote_count"
CREDITS_HEADER = "cast,crew,id"
RATINGS_HEADER = "userId,movieId,rating,timestamp"

FIELDS_COUNT_MOVIES = len(MOVIES_HEADER.split(","))
FIELDS_COUNT_CREDITS = len(CREDITS_HEADER.split(","))
FIELDS_COUNT_RATINGS = len(RATINGS_HEADER.split(","))

FIELD_SEPARATOR = "|"
VALUE_SEPARATOR = ","
LINE_SEPARATOR = "\n"


class Processor:
    """
    Base class for file processors.
    Parses chunks of data and ensures the data is valid.
    Stores the data in a HasherContainer for sending to the appropiate nodes.
    """

    def __init__(self, positions_for_hasher: dict[str, int]):
        """
        Initializes the file processor.
        :param positions_for_hasher: positions for the hasher
        """
        self.container = HasherContainer(positions_for_hasher)
        self.header_length = 0
        self.fields_count = 0

        self.bytes_read = 0
        self.read_until = 0
        self.errors_per_file = 0
        self.successful_lines_count = 0

    def process_first_batch(self, bytes_received: int, chunck_received: str):
        """
        Processes the first received batch of data, which contains the file size and the header.
        :param bytes_received: number of bytes received
        :param chunck_received: chunk of data to process
        """
        index_delimiter = chunck_received.find("|")

        file_size = int(chunck_received[:index_delimiter])
        self.read_until = file_size
        chunck_received = chunck_received[index_delimiter + 1 :]

        chunck_received = self.remove_header(chunck_received)
        self.process_batch(bytes_received, chunck_received)

    def process_batch(self, bytes_received: int, chunck_received: str):
        """
        Parses and stores a chunk of data.
        :param bytes_received: number of bytes received
        :param chunck_received: chunk of data to process
        """
        self.bytes_read += bytes_received
        successful_lines_count = 0
        error_count = 0
        reader = csv.reader(io.StringIO(chunck_received))

        for row in reader:
            try:
                if len(row) == 0 or len(row) != self.fields_count:
                    error_count += 1
                    logging.debug(
                        f"Error processing line, Expected {self.fields_count} fields, got {len(row)}"
                    )
                    continue
                movie_id, line_processed = self._process_line(row)
                successful_lines_count += 1
                self.container.append_to_node(movie_id, line_processed)
            except Exception as e:
                error_count += 1
                logging.debug(f"Error processing line, Error: {e}")
                continue
        self.successful_lines_count += successful_lines_count
        self.errors_per_file += error_count

    def received_all_data(self) -> bool:
        """
        Checks if all data has been received.
        :return: True if all data has been received, False otherwise
        """
        return self.bytes_read >= self.read_until

    def remove_header(self, csv_data: str) -> str:
        """
        Removes the header from the data.
        :param csv_data: data to remove the header from
        :return: data without the header
        """
        return csv_data[self.header_length :]

    def _process_line(self, line: list[str]) -> list[int, str]:
        """
        Parses a single line of data.
        :param line: line of data to process
        :return: tuple of id and processed line
        """
        raise NotImplementedError("Subclasses should implement this method")

    def _try_parse_python_structure(self, text: str):
        """
        Tries to parse a python structure from a string.
        :param text: string to parse
        :return: parsed python structure
        :raises: ValueError if the string is not a valid python structure
        """
        try:
            return ast.literal_eval(text)
        except (ValueError, SyntaxError):
            return ""

    def convert_movie_id_to_int(self, movie_id: str) -> int:
        """
        Converts an id in string format to an integer.
        :param movie_id: id in string format
        :return: id in integer format
        :raises: EmptyFieldError if the id is not a valid integer
        """
        try:
            return int(movie_id)
        except ValueError:
            raise EmptyFieldError(f"Invalid movie_id: {movie_id}")

    def get_processed_batch(self) -> dict[str, dict[int, str]]:
        return self.container.get_buffers()


class MoviesProcessor(Processor):
    """
    Processor for movies data. See base class for more information.
    """

    def __init__(self, positions_for_hasher):
        super().__init__(positions_for_hasher)
        self.header_length = len(MOVIES_HEADER) + 1  # +1 for the \n
        self.fields_count = FIELDS_COUNT_MOVIES

    def _process_line(self, line: list[str]) -> list[int, str]:
        budget = line[2]
        genres = line[3]
        movie_id = line[5]
        prodCountries = line[13]
        releaseDate = line[14]
        title = line[20]
        spokenLanguages = line[18]
        revenue = line[15]
        overview = line[9]

        if LINE_SEPARATOR in overview:
            overview = overview.split(LINE_SEPARATOR)
            overview = " ".join(overview)

        prodCountries = self._try_parse_python_structure(prodCountries)
        genres = self._try_parse_python_structure(genres)

        if not movie_id:
            raise EmptyFieldError("Missing movie_id")
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

        countries = VALUE_SEPARATOR.join([c["iso_3166_1"] for c in prodCountries])
        genres = VALUE_SEPARATOR.join([g["name"] for g in genres])

        movie_id = self.convert_movie_id_to_int(movie_id)
        return (
            movie_id,
            f"{movie_id}{FIELD_SEPARATOR}{title}{FIELD_SEPARATOR}{releaseDate}{FIELD_SEPARATOR}{countries}{FIELD_SEPARATOR}{genres}{FIELD_SEPARATOR}{budget}{FIELD_SEPARATOR}{overview}{FIELD_SEPARATOR}{revenue}{LINE_SEPARATOR}",
        )


class CreditsProcessor(Processor):
    """
    Processor for credits data. See base class for more information.
    """

    def __init__(self, positions_for_hasher):
        super().__init__(positions_for_hasher)
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
            chunck_received = chunck_received[index_delimiter + 1 :]
            # 4 bytes for the row size + 1 byte for the delimiter
            bytes_received -= len(str_length) + 1
        self.row_buffer += chunck_received
        if len(self.row_buffer) == self.row_length:
            super().process_batch(bytes_received, self.row_buffer)
            self.row_buffer = ""
            self.row_length = 0
        else:
            self.bytes_read += bytes_received

    def _process_line(self, line: list[str]) -> list[int, str]:
        cast = line[0]
        movie_id = line[2]

        cast = self._try_parse_python_structure(cast)
        if not cast:
            raise EmptyFieldError("Missing cast")
        if not movie_id:
            raise EmptyFieldError("Missing movie_id")

        cast = VALUE_SEPARATOR.join([c["name"] for c in cast])
        movie_id = self.convert_movie_id_to_int(movie_id)
        return movie_id, f"{movie_id}{FIELD_SEPARATOR}{cast}{LINE_SEPARATOR}"


class RatingsProcessor(Processor):
    """
    Processor for ratings data. See base class for more information.
    """

    def __init__(self, positions_for_hasher):
        super().__init__(positions_for_hasher)
        self.header_length = len(RATINGS_HEADER) + 1  # +1 for the \n
        self.fields_count = FIELDS_COUNT_RATINGS

    def _process_line(self, line: list[str]) -> list[int, str]:
        movie_id = line[1]
        rating = line[2]
        timestamp = line[3]

        if not movie_id:
            raise EmptyFieldError("Missing movie_id")
        if not rating:
            raise EmptyFieldError("Missing rating")
        if not timestamp:
            raise EmptyFieldError("Missing timestamp")

        movie_id = self.convert_movie_id_to_int(movie_id)
        return movie_id, f"{movie_id}{FIELD_SEPARATOR}{rating}{LINE_SEPARATOR}"


class EmptyFieldError(Exception):
    """
    Exception raised when a required field is empty.
    """

    pass
