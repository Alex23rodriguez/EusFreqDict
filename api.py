import polars as pl

# Load data once at startup
data = pl.read_parquet("./assets/data.parquet")


def freq(words: str):
    return (
        data.filter(pl.col("word").is_in(words.split()))
        .select(
            "index", "word", "freq", "k_low", "k_high", "k_low_total", "k_high_total"
        )
        .to_dicts()
    )
