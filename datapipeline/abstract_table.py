import hashlib
from abc import ABC, abstractmethod
from pathlib import Path
from collections.abc import Callable
from typing import Any, ClassVar

import pandas as pd


class AbstractTable(ABC):
    temp_directory: Path
    row_limit_size: int
    batch_size: int
    debug: bool
    created_parquet_files: ClassVar[list[str]] = []

    class FileProcessingError(Exception):
        pass

    class DataTransformationError(Exception):
        pass

    @classmethod
    @abstractmethod
    def process_table(cls, file_name: str, second_file_name: str | None) -> None:
        pass

    @classmethod
    def _generate_id(cls, column: str) -> str | None:
        if pd.isna(column):
            return column
        return hashlib.md5(column.encode("utf-8")).hexdigest()

    @classmethod
    def _generate_synthetic_id(cls, row: dict[str, Any]) -> str:
        combined_values = row["tconst"] + str(row["ordering"]) + row["nconst"]
        return hashlib.md5(combined_values.encode("utf-8")).hexdigest()

    @classmethod
    def _read_tsv_into_dataframe(cls, file_name: str) -> pd.DataFrame:
        print(f"Reading {file_name} into dataframe...")
        return pd.read_csv(Path(cls.temp_directory, file_name),
                           sep="\t", low_memory=False,
                           usecols=["tconst"],
                           )

    @classmethod
    def _replace_chars(cls, text: str) -> str:
        if isinstance(text, str):
            return text.replace("[\"", "").replace("\"]", "")
        return text

    @classmethod
    def _process_batches(
            cls,
            file_name: str,
            second_file_name: str | None,
            _process_batch: Callable,
    ) -> None:

        df = pd.read_csv(Path(cls.temp_directory, file_name), sep="\t", usecols=[0])
        total_rows = df.shape[0]
        del df
        num_batches = total_rows // cls.batch_size  # type: ignore

        df2 = cls._read_tsv_into_dataframe(second_file_name) if second_file_name is not None else None

        if cls.debug:
            num_batches = 1

        file_suffix = 0

        for i in range(num_batches):
            file_suffix += 1
            skip_rows = i * cls.batch_size
            file_suffix += 1
            _process_batch(
                file_name,
                str(file_suffix).zfill(2),
                cls.batch_size,
                skip_rows,
                df2 if df2 is not None else None,
            )

        remaining_rows = total_rows % cls.batch_size
        if remaining_rows > 0:
            skip_rows = num_batches * cls.batch_size
            _process_batch(
                file_name,
                str(file_suffix).zfill(2),
                remaining_rows,
                skip_rows,
                df2 if df2 is not None else None,
            )

        del df2

    @classmethod
    def _save_to_parquet(cls, df: pd.DataFrame, file_suffix: str | None = None) -> None:

        try:
            file_name = f"{cls.__name__}.parquet" if file_suffix is None else f"{cls.__name__}_{file_suffix}.parquet"
            file_path = Path(cls.temp_directory, file_name)
            print(f"Saving DataFrame to Parquet: {file_path}")
            df.to_parquet(file_path, index=False)
            print("DataFrame saved successfully.")

        except Exception as e:
            error_message = f"An error occurred while saving DataFrame to Parquet: {e}"
            raise cls.FileProcessingError(error_message) from None
