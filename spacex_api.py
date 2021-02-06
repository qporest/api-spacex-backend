import typer
import json
import psycopg2
from pgcopy import CopyManager
from collections import namedtuple
from datetime import datetime, timezone

DB_COLUMNS = namedtuple("DB_COLUMNS", ["recorded_at", "satelite_id","longitude","latitude"])
SATELITE_COLUMS = namedtuple("SATELITE_COLUMNS", ["id", "name"])


app = typer.Typer()


def create_connection_string(username: str, password: str, host: str, port: int, db: str):
	"""Creates a connection string for PostgreSQL"""
	return f"postgres://{username}:{password}@{host}:{str(port)}/{db}"


def get_db_connection(username: str = None, password: str = None, host: str = None, port: int = None, db: str = None):
	"""Create a psycopg2 connection to DB or return None"""
	connection = None
	try:
		connection = psycopg2.connect(create_connection_string(username, password, host, port, db))
	except psycopg2.OperationalError as e:
		typer.echo(str(e), err=True)
	return connection


def satelite_position_record_factory(json_obj):
	"""
	We're using a few assumptions here:
	In the records I've seen TIME_SYSTEM is "UTC", so we'll assume it for now
	"id" isn't in schema, but appears in the record, and is nice for PostgreSQL to use as Primary key and index on.
	"""
	return DB_COLUMNS(
		recorded_at=datetime.strptime(json_obj["spaceTrack"]["EPOCH"], "%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=timezone.utc).timestamp(),
		satelite_id=json_obj["id"],
		longitude=json_obj["longitude"],
		latitude=json_obj["latitude"]
	)

def satelite_record_factory(json_obj):
	return SATELITE_COLUMS(
		id=json_obj["id"],
		name=json_obj["spaceTrack"]["OBJECT_NAME"]
	)

def convert_api_to_rows(json_data, factory):
	return [factory(x) for x in json_data]

def import_json_data(data=None, connection=None):
	"""
	Takes in result of StarlinkAPI and copies it into DB.
	First - need to get all of the satelites and insert them to not break the foreign key constraint.
	Then add all of the data.
	Returns success status
	"""
	satelites = convert_api_to_rows(data, satelite_record_factory)
	satelite_positions = convert_api_to_rows(data, satelite_position_record_factory)



@app.command()
def import_data(file: str = typer.Option("", help="JSON file to load the historic data from"), 
		stdin: bool = typer.Option(False, envvar="POSTGRES_HOST", help="If you want to pipe in data instead of reading from a file."),
		postgres_host: str = typer.Option(..., envvar="POSTGRES_HOST", prompt=True),
		postgres_port: int = typer.Option(..., envvar="POSTGRES_POST", prompt=True),
		postgres_user: str = typer.Option(..., envvar="POSTGRES_USER", prompt=True),
		postgres_password: str = typer.Option(..., envvar="POSTGRES_PASSWORD", prompt=True, hide_input=True),
		postgres_db: str = typer.Option(..., envvar="POSTGRES_DB", prompt=True),
	):
	if not (stdin or file):
		typer.secho("Need to provide either a file to read from or specify --stdin", fg=typer.colors.RED, err=True)
		raise typer.Exit(code=1)
	if stdin and file:
		typer.secho("Only provide filename or stdin at a time. Exiting because of ambiguity", fg=typer.colors.RED, err=True)
		raise typer.Exit(code=1)

	if file:
		with open(file, 'r') as input_json:
			data = json.load(input_json)
	else:
		pass

	db_connection = get_db_connection(username=postgres_user, password=postgres_password, 
		host=postgres_host, port=postgres_port, db=postgres_db)
	if not db_connection:
		typer.secho("Couldn't connect to DB", fg=typer.colors.RED, err=True)
		raise typer.Exit(code=1)

	import_json_data(data=data, connection=db_connection)


if __name__ == "__main__":
    app()