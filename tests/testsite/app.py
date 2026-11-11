import time
from pathlib import Path

from flask import Flask, abort

app = Flask(__name__)


def read_file(filename: Path):
    with filename.open() as f:
        return f.read()


@app.route("/")
def base():
    return b"hello"


@app.route("/index.html")
def index():
    return read_file(Path("index.html"))


@app.route("/page1.html")
def page1():
    return read_file(Path("page1.html"))


@app.route("/page2.html")
def page2():
    return read_file(Path("page2.html"))


@app.route("/page3.html")
def page3():
    return read_file(Path("page3.html"))


@app.route("/hang")
def hang():
    """Never answers in time; used to test that the crawl deadline fires."""
    time.sleep(60)
    return b"too late"


@app.route("/err/<int:code>")
def return_code(code):
    abort(code)
