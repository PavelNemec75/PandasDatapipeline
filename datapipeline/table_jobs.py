import numpy as np
import pandas as pd
from pathlib import Path

from .abstract_table import AbstractTable


class Jobs(AbstractTable):
    @classmethod
    def process_table(cls, file_name: str, second_file_name: str | None) -> None:

        _ = second_file_name

        print(f"Processing table: {cls.__name__}, file: {file_name}", flush=True)

        try:
            # read title.principals.tsv - only column jobs into Pandas series, filter unique values and create dataframe
            s = pd.read_csv(
                Path(cls.temp_directory, file_name),
                sep="\t",
                usecols=["job"],
                nrows=cls.row_limit_size if cls.debug else None,
            )["job"]
            s = s.unique()
            df = pd.DataFrame({"job": s})

        except (FileNotFoundError, Exception) as e:
            error_message = f"File {file_name} not found: {e}"
            raise cls.FileProcessingError(error_message) from None

        try:

            df = (
                df
                .replace({"\\N": np.nan})
                .dropna(subset=["job"], how="all")
                .assign(
                    id=df["job"].apply(lambda x: cls._generate_id(x) if x is not None else None),
                    job=df["job"].str.slice(0, 36).str.title().str.replace("_", " "),
                )
                .reindex(columns=["id", "job"])
                .drop_duplicates(subset=["id", "job"])
                .sort_values(by=["job"])
                .reset_index(drop=True)
            )

        except Exception as e:
            error_message = f"File {file_name} not found: {e}"
            raise cls.DataTransformationError(error_message) from None

        cls._save_to_parquet(df)

        print(f"Finished table {cls.__name__}, file: {file_name}", flush=True)
