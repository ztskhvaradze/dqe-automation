import pandas as pd

class ParquetReader:
    def __init__(self):
        pass

    def process(self, file_path: str):
        """
        Reads a Parquet file and returns a pandas DataFrame.
        """
        try:
            df = pd.read_parquet(file_path)
            return df
        except Exception as e:
            print(f"Error reading Parquet file: {e}")
            raise
