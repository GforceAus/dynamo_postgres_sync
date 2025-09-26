import typer

app = typer.Typer()


@app.command()
def extract():
    """Extract data from source."""
    print("Extracting data...")


@app.command()
def transform():
    """Transform the extracted data."""
    print("Transforming data...")


@app.command()
def load():
    """Load data into destination."""
    print("Loading data...")


if __name__ == "__main__":
    app()
