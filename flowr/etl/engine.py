from typing import Callable, List

import polars as pl


class ETLFlow:
    def __init__(
        self,
        source_connector: Callable[..., pl.DataFrame],
        transform: List[Callable[..., pl.DataFrame]],
        destination_connector: Callable[..., int],
    ):
        self.source_connector = source_connector
        self.transform = transform
        self.destination_connector = destination_connector

    def run(self, *extract_args, **extract_kwargs):
        try:
            # Extract.
            print("Extraction...")
            df = self.source_connector(*extract_args, **extract_kwargs)
            print(f"Data extracted: {df.shape}")

            # Transform.
            for tf in self.transform:
                print(f"Applying transform: {tf.__name__}")
                df = tf(df)

            # Load.
            print("Loading...")
            result = self.destination_connector(df)

            print("Integration finished.")

            return result
        except Exception as e:
            print(f"Error in ETL pipline: {e}")
            raise
