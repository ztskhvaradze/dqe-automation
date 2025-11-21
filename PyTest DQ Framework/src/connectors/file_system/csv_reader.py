import csv
import logging

class CsvReader:
    """
    Reads CSV files and returns data as a list of dictionaries.
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def process(self, file_path: str):
        """
        Reads a CSV file and returns its contents as a list of dicts.
        """
        self.logger.info(f"Reading CSV file: {file_path}")

        try:
            with open(file_path, mode="r", encoding="utf-8") as file:
                reader = csv.DictReader(file)
                return list(reader)

        except FileNotFoundError:
            self.logger.error(f"CSV file not found: {file_path}")
            raise
        except Exception as e:
            self.logger.error(f"Error reading CSV: {str(e)}")
            raise
