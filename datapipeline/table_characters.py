import numpy as np
import pandas as pd
from pathlib import Path

from .abstract_table import AbstractTable


class Characters(AbstractTable):

    @classmethod
    def _process_batch(cls, file_name: str, file_suffix: str, nrows: int, skiprows: int,  # noqa: PLR0913
                       df2: None) -> None:

        _ = df2

        print(f"Processing: {file_name}_{file_suffix}, {nrows:n}:{skiprows:n}")

        try:
            # read title.principals.tsv as dataframe
            df = pd.read_csv(
                Path(cls.temp_directory, file_name),
                sep="\t",
                usecols=[5],
                nrows=nrows,
                skiprows=skiprows,
            )
            df.columns = ["characters"]

        except (FileNotFoundError, Exception) as e:
            error_message = f"File {file_name} not found: {e}"
            raise cls.FileProcessingError(error_message) from None

        try:

            df = (
                df
                .assign(
                    character=df["characters"].apply(
                        lambda json_str: json_str.replace("[\"", "")
                        .replace("\"]", "") if isinstance(json_str, str) else json_str)
                    .str.split(","),
                )
                .explode("character")
                .replace({"\\N": np.nan})  # fix null values
                .dropna(subset=["character"], how="all")
                .assign(
                    id=lambda x: x["character"].apply(lambda x: cls._generate_id(x)).astype(str),
                )
                .drop("characters", axis=1)
                .reindex(columns=["id", "character"])
                .drop_duplicates(subset=["id", "character"])
                .sort_values(by=["character"])
                .reset_index(drop=True)
            )

        except Exception as e:
            error_message = f"File {file_name} not found: {e}"
            raise cls.DataTransformationError(error_message) from None

        cls._save_to_parquet(df, file_suffix)
        print(f"Finished table {cls.__name__}, file: {file_name}", flush=True)

    @classmethod
    def process_table(cls, file_name: str, second_file_name: str | None) -> None:

        _ = second_file_name

        print(f"Processing table: {cls.__name__}, file: {file_name}", flush=True)

        cls._process_batches(file_name, None, cls._process_batch)
