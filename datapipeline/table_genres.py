import numpy as np
import pandas as pd
from pathlib import Path

from .abstract_table import AbstractTable


class Genres(AbstractTable):
    @classmethod
    def process_table(cls, file_name: str, second_file_name: str | None) -> None:

        _ = second_file_name

        print(f"Processing table: {cls.__name__}, file: {file_name}", flush=True)

        try:
            # read title.basics.tsv as dataframe
            df = pd.read_csv(
                Path(cls.temp_directory, file_name),
                sep="\t",
                low_memory=False,
                usecols=["genres"],
                nrows=cls.row_limit_size if cls.debug else None,
            )

        except (FileNotFoundError, Exception) as e:
            error_message = f"File {file_name} not found: {e}"
            raise cls.FileProcessingError(error_message) from None

        try:

            df = (
                df
                .assign(
                    genre=df["genres"].str.split(","),
                    id=df["genres"].str.split(",").apply(lambda x: cls._generate_id(x[0]) if x else None),
                )
                .explode("genre")  # unnest genres column
                .dropna(subset=["genre"])  # remove null values
                .drop_duplicates(subset=["genre"])
                .replace({"\\N": np.nan})
                .sort_values(by=["genre"])
                .reset_index(drop=True)
                .reindex(columns=["id", "genre"])
            )

        except Exception as e:
            error_message = f"File {file_name} not found: {e}"
            raise cls.DataTransformationError(error_message) from None

        cls._save_to_parquet(df)
        print(f"Finished table {cls.__name__}, file: {file_name}", flush=True)
