import numpy as np
import pandas as pd
from pathlib import Path

from .abstract_table import AbstractTable


class Episodes(AbstractTable):
    @classmethod
    def process_table(cls, file_name: str, second_file_name: str | None) -> None:

        print(f"Processing table: {cls.__name__}, file: {file_name}", flush=True)

        try:
            # read title.episode.tsv as dataframe
            title_episodes = pd.read_csv(
                Path(cls.temp_directory, file_name),
                sep="\t",
                usecols=["tconst", "parentTconst", "seasonNumber", "episodeNumber"],
                nrows=cls.row_limit_size if cls.debug else None,
            )

            # read title.basics.tsv as dataframe
            title_basics = pd.read_csv(
                Path(cls.temp_directory, second_file_name),  # type: ignore
                sep="\t",
                low_memory=False,
                usecols=["tconst"],
            )

        except (FileNotFoundError, Exception) as e:
            error_message = f"File {file_name} not found: {e}"
            raise cls.FileProcessingError(error_message) from None

        # inner join
        df = pd.merge(title_basics, title_episodes, on="tconst", how="inner")  # noqa: PD015
        del title_basics, title_episodes

        try:

            df = (
                df
                .replace({"\\N": np.nan})
                .dropna(subset=["seasonNumber", "episodeNumber"], how="all")
                .rename(columns={"tconst": "id", "parentTconst": "episode_id", "seasonNumber": "season_number",
                                 "episodeNumber": "episode_number"})
                .drop_duplicates(subset=["id", "episode_id"])
                .astype({"id": str, "episode_id": str, "season_number": int, "episode_number": int})
            )

        except Exception as e:
            error_message = f"File {file_name} not found: {e}"
            raise cls.DataTransformationError(error_message) from None

        cls._save_to_parquet(df)
        print(f"Finished table {cls.__name__}, file: {file_name}", flush=True)
