import polars as pl

data = pl.read_parquet("./assets/data.parquet")


def get(words):
    return (
        data.filter(pl.col("word").is_in(words.split()))
        .select("word", "freq")
        .to_dicts()
    )
