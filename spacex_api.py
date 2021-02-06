import typer
import psycopg2
from pgcopy import CopyManager

app = typer.Typer()

def create_connection_string(username: str, password: str, host: str, port: int, dbname: str):
	return f"postgres://{username}:{password}@{host}:{str(port)}/{dbname}"

def get_db_connection(username: str, password: str, host: str, port: int, dbname: str):
	return psycopg2.connect(create_connection_string(username, password, host, port, dbname))

@app.command()
def import_data(file: str = typer.Argument(...), 
		stdin: str = typer.Option(..., envvar="POSTGRES_HOST", prompt=True, help="If you want to pipe in data instead of reading from a file."),
		postgres_host: str = typer.Option(..., envvar="POSTGRES_HOST", prompt=True),
		postgres_port: int = typer.Option(..., envvar="POSTGRES_POST", prompt=True),
		postgres_user: str = typer.Option(..., envvar="POSTGRES_USER", prompt=True),
		postgres_password: str = typer.Option(..., envvar="POSTGRES_PASSWORD", prompt=True, hide_input=True),
	):
	typer.echo("I was launched")

if __name__ == "__main__":
    app()