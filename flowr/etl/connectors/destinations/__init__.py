import polars as pl


def load_csv(df: pl.DataFrame, path: str) -> int:
    df.write_csv(path)
    return len(df)
