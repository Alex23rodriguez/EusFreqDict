from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse, HTMLResponse
import polars as pl

app = FastAPI()

# Load data once at startup
data = pl.read_parquet("./assets/data.parquet")


def get(words: str):
    return (
        data.filter(pl.col("word").is_in(words.split()))
        .select("word", "freq")
        .to_dicts()
    )


@app.get("/api/freq")
def get_freq(words: str = Query(..., description="Space-separated words")):
    return JSONResponse(get(words))


# Serve the frontend
@app.get("/", response_class=HTMLResponse)
def index():
    with open("templates/index.html", "r", encoding="utf-8") as f:
        return f.read()
