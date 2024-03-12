import numpy as np
import pandas as pd
from pathlib import Path

from .abstract_table import AbstractTable


class PrincipalsCharacters(AbstractTable):

    @classmethod
    def _process_batch(cls, file_name: str, file_suffix: str, nrows: int, skiprows: int,  # noqa: PLR0913
                       df2: pd.DataFrame) -> None:

        print(f"Processing: {file_name}_{file_suffix}, {nrows:n}:{skiprows:n}")

        try:
            # read title.principals.tsv as dataframe
            title_principals = pd.read_csv(
                Path(cls.temp_directory, file_name),  # type: ignore
                sep="\t",
                usecols=[0, 1, 2, 5],
                nrows=nrows,
                skiprows=skiprows,
            )
            title_principals.columns = ["tconst", "ordering", "nconst", "characters"]  # type: ignore

        except (FileNotFoundError, Exception) as e:
            error_message = f"File {file_name} not found: {e}"
            raise cls.FileProcessingError(error_message) from None

        try:

            # title_basics dataframe
            title_basics = df2

            # inner join
            df = pd.merge(title_basics, title_principals, on="tconst", how="inner")  # noqa: PD015
            del title_basics, title_principals

            df = (
                df
                .assign(
                    id=df.apply(lambda row: cls._generate_synthetic_id(row), axis=1),
                    character=df["characters"].apply(
                        lambda json_str: json_str
                        .replace("[\"", "")
                        .replace("\"]", "") if isinstance(json_str, str) else json_str  # noqa: COM812
                    ).str.split(","),
                )
                .explode("character")
                .drop(["tconst", "ordering", "nconst"], axis=1)
                .replace({"\\N": np.nan})
                .dropna(subset=["character"])
                .drop_duplicates(subset=["id", "character"])
                .assign(
                    character_id=lambda x: x["character"].apply(lambda x: cls._generate_id(x)).astype(str),
                )
                .drop(["character", "characters"], axis=1)
                .dropna(subset=["character_id"])
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
