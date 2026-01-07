from pprint import pprint

import polars as pl
from polars import col

# %%
data = pl.read_csv("./assets/above_average.csv")

# %%
df = data.with_columns(
    pl.struct(
        pl.col("neigh").str.split(" ").alias("all"),
        pl.col("neigh_sub").str.split(" ").alias("sub"),
        pl.col("neigh_del").str.split(" ").alias("del"),
        pl.col("neigh_add").str.split(" ").alias("add"),
        pl.col("neigh_transpose").str.split(" ").alias("tr"),
        pl.col("neigh_hf").str.split(" ").alias("hf"),
        pl.col("neigh_hf_sub").str.split(" ").alias("hf_sub"),
        pl.col("neigh_hf_del").str.split(" ").alias("hf_del"),
        pl.col("neigh_hf_add").str.split(" ").alias("hf_add"),
        pl.col("neigh_hf_transpose").str.split(" ").alias("hf_tr"),
    ).alias("neighbors")
).select(
    pl.row_index("index"),
    "word",
    "amount",
    "freq",
    "log",
    "length",
    "num_syllables",
    "syllables",
    "cv_structure",
    "OUP",
    "neighbors",
)

# %%
# add percentile
# %%
# we don't do this because it gives different value to words with the same amount, alphabetically
# df.select(col("amount").cum_sum(reverse=True) / col("amount").sum())

# %%
# instead, we group by amount first, and we calculate the percentile over the group
t = (
    df.group_by("amount")
    .agg(pl.len())
    .sort("amount", descending=True)
    .with_columns((col("amount") * col("len")).alias("total"))
    .with_columns(
        (col("total").cum_sum(reverse=True) / col("total").sum() * 100).alias("k_high")
    )
    .with_columns(col("k_high").shift(-1, fill_value=0).alias("k_low"))
    .select("amount", "k_low", "k_high")
)

# %%
df = df.join(t, on="amount", how="left")


# %%
# we also want the percentile over all words, not just the high freq ones
all_words = pl.read_csv("./assets/full.csv")

# %%
t = (
    all_words.group_by("amount")
    .agg(pl.len())
    .sort("amount", descending=True)
    .with_columns((col("amount") * col("len")).alias("total"))
    .with_columns(
        (col("total").cum_sum(reverse=True) / col("total").sum() * 100).alias(
            "k_high_total"
        )
    )
    .with_columns(col("k_high_total").shift(-1, fill_value=0).alias("k_low_total"))
    .select("amount", "k_low_total", "k_high_total")
)


# %%
df = df.join(t, on="amount", how="left")


# %%
df.write_parquet("./assets/data.parquet")
