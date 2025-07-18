import polars as pl


def extract_csv(path: str) -> pl.DataFrame:
    return pl.read_csv(path)
