import numpy as np
import pandas as pd
from pathlib import Path

from .abstract_table import AbstractTable


class Principals(AbstractTable):

    @classmethod
    def _process_batch(cls, file_name: str, file_suffix: str, nrows: int, skiprows: int,  # noqa: PLR0913
                       df2: pd.DataFrame) -> None:

        _ = df2

        print(f"Processing: {file_name}_{file_suffix}, {nrows:n}:{skiprows:n}")

        try:
            # read title.principals.tsv as dataframe
            df = pd.read_csv(
                Path(cls.temp_directory, file_name),
                sep="\t",
                usecols=[0, 1, 2, 3, 4],
                nrows=nrows,
                skiprows=skiprows,
            )
            df.columns = ["tconst", "ordering", "nconst", "category", "job"]

        except (FileNotFoundError, Exception) as e:
            error_message = f"File {file_name} not found: {e}"
            raise cls.FileProcessingError(error_message) from None

        try:

            df = (
                df
                .assign(
                    job=df["job"].str.slice(0, 36).str.title().str.replace("_", " "),
                    id=df.apply(lambda row: cls._generate_synthetic_id(row), axis=1),
                    category_id=df["category"].apply(lambda x: cls._generate_id(x) if x is not None else None),
                    job_id=df["job"].apply(lambda x: cls._generate_id(x) if x is not None else None),
                )
                .replace({"\\N": np.nan})
                .rename(columns={"tconst": "title_id", "nconst": "person_id"})
                .reindex(columns=["id", "title_id", "ordering", "person_id", "category_id",
                                  "job_id"])
                .drop_duplicates(subset=["id"])
                .astype({
                    "id": str,
                    "title_id": str,
                    "ordering": int,
                    "person_id": str,
                    "category_id": str,
                    "job_id": str,
                })
            )

        except Exception as e:
            error_message = f"File {file_name} not found: {e}"
            raise cls.DataTransformationError(error_message) from None

        cls._save_to_parquet(df, file_suffix)
        print(f"Finished table {cls.__name__}, file: {file_name}", flush=True)

    @classmethod
    def process_table(cls, file_name: str, second_file_name: str | None) -> None:

        print(f"Processing table: {cls.__name__}, file: {file_name}", flush=True)

        cls._process_batches(file_name, second_file_name, cls._process_batch)
