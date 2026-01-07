from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse

from api import freq

app = FastAPI(root_path="/freq")


@app.get("/api/freq")
def get_freq(words: str = Query(..., description="Space-separated words")):
    return JSONResponse(freq(words))


# Serve the frontend
@app.get("/", response_class=HTMLResponse)
def index():
    with open("templates/index.html", "r", encoding="utf-8") as f:
        return f.read()
