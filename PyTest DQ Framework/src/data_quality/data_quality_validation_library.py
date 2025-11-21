class DataQualityLibrary:
    """
    A library for performing basic data quality checks.
    Each method raises ValueError if the check fails.
    """

    @staticmethod
    def check_dataset_is_not_empty(dataset):
        """
        Raises ValueError if the dataset is empty.
        Works with pandas DataFrames or list of dicts.
        """
        if hasattr(dataset, "empty"):  # pandas DataFrame
            if dataset.empty:
                raise ValueError("Dataset is empty!")
        else:  # assume list-like
            if not dataset:
                raise ValueError("Dataset is empty!")
        return True

    @staticmethod
    def check_count(source_data, target_data):
        if len(source_data) != len(target_data):
            raise ValueError(
                f"Row count mismatch: source({len(source_data)}), target({len(target_data)})"
            )
        return True

    @staticmethod
    def check_required_columns(dataset, required_columns):
        # Handle DataFrame
        if hasattr(dataset, "columns"):
            dataset_columns = set(dataset.columns)
        elif isinstance(dataset, (list, tuple)) and dataset:
            dataset_columns = set(dataset[0].keys())
        else:
            raise ValueError("Dataset is empty or unsupported type")

        missing_columns = set(required_columns) - dataset_columns
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        return True

    @staticmethod
    def check_duplicates(dataset, column_names=None):
        if hasattr(dataset, "duplicated"):  # DataFrame
            if column_names:
                if dataset.duplicated(subset=column_names).any():
                    raise ValueError(f"Duplicate rows found on columns: {column_names}")
            else:
                if dataset.duplicated().any():
                    raise ValueError("Duplicate rows found in dataset")
        elif isinstance(dataset, (list, tuple)):
            seen = set()
            for row in dataset:
                key = tuple(row[col] for col in column_names) if column_names else tuple(row.items())
                if key in seen:
                    raise ValueError("Duplicate rows found in dataset")
                seen.add(key)
