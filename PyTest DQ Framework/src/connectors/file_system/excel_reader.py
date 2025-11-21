import pandas as pd
import logging

class ExcelReader:
    """
    Reads Excel files and returns data as a list of dictionaries.
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def process(self, file_path: str, sheet_name: str = 0):
        """
        Reads an Excel file and returns its contents as a list of dicts.
        """
        self.logger.info(f"Reading Excel file: {file_path}, sheet: {sheet_name}")

        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            return df.to_dict(orient="records")
        except FileNotFoundError:
            self.logger.error(f"Excel file not found: {file_path}")
            raise
        except Exception as e:
            self.logger.error(f"Error reading Excel file: {str(e)}")
            raise
