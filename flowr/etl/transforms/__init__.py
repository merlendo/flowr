import polars as pl


def multiply_column(df: pl.DataFrame, col_name: str, factor: float) -> pl.DataFrame:
    return df.with_columns((pl.col(col_name) * factor).alias(f"{col_name}_multiplied"))
