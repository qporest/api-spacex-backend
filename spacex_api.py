import typer
import json
import psycopg2
from pgcopy import CopyManager
from collections import namedtuple
from datetime import datetime, timezone
from dateutil import parser, tz
import decimal

SATELITE_TABLE = "satelites"
SATELITE_POS_TABLE = "satelite_positions"
SAT_POSITION_COLUMNS = namedtuple("SAT_POSITION_COLUMNS", ["recorded_at", "satelite_id","longitude","latitude"])
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
		Note: it's invalid UUID, so had to make it TEXT
	"""
	UTC = tz.gettz("UTC")
	if json_obj["longitude"] is None or json_obj["latitude"] is None:
		return None
	return SAT_POSITION_COLUMNS(
		recorded_at=parser.parse(json_obj["spaceTrack"]["EPOCH"]).astimezone(tz.gettz("UTC")),
		satelite_id=json_obj["id"].strip(),
		longitude=decimal.Decimal(json_obj["longitude"]),
		latitude=decimal.Decimal(json_obj["latitude"])
	)

def satelite_record_factory(json_obj):
	return SATELITE_COLUMS(
		id=json_obj["id"].strip(),
		name=json_obj["spaceTrack"]["OBJECT_NAME"].strip()
	)

def get_unique_only(records, index_to_compare):
	cache = {}
	result = []
	for record in records:
		if record[index_to_compare] not in cache:
			cache[record[index_to_compare]] = True
			result.append(record)
	return result

def convert_api_to_rows(json_data, factory):
	return [factory(x) for x in json_data if factory(x) is not None]

def copy_data(records, connection, table, fields):
	try:
		mgr = CopyManager(connection, table, fields)
		mgr.copy(records)
		connection.commit()
	except ValueError as e:
		print(str(e)) # TODO: remove
		raise e
		typer.echo(str(e), err=True)
		return False
	return True

def import_data(data=None, connection=None):
	"""
	Takes in result of StarlinkAPI and copies it into DB.
	First - need to get all of the satelites and insert them to not break the foreign key constraint.
	Then add all of the data.
	Returns success status
	"""
	satelite_data = convert_api_to_rows(data, satelite_record_factory)
	satelite_data = get_unique_only(satelite_data, SATELITE_COLUMS._fields.index("id"))
	success = copy_data(satelite_data, connection, SATELITE_TABLE, SATELITE_COLUMS._fields)
	if not success:
		typer.secho("Couldn't import satelites", fg=typer.colors.RED, err=True)
		raise typer.Exit(code=1)
	
	satelite_positions = convert_api_to_rows(data, satelite_position_record_factory)
	success = copy_data(satelite_positions, connection, SATELITE_POS_TABLE, SAT_POSITION_COLUMNS._fields)
	if not success:
		typer.secho("Couldn't import satelite positions", fg=typer.colors.RED, err=True)
		raise typer.Exit(code=1)
	
	return True

@app.command("import")
def import_data_command(file: str = typer.Option("", help="JSON file to load the historic data from"), 
		stdin: bool = typer.Option(False, envvar="POSTGRES_HOST", help="If you want to pipe in data instead of reading from a file."),
		postgres_host: str = typer.Option(..., envvar="POSTGRES_HOST", prompt=True),
		postgres_port: int = typer.Option(..., envvar="POSTGRES_POST", prompt=True),
		postgres_user: str = typer.Option(..., envvar="POSTGRES_USER", prompt=True),
		postgres_password: str = typer.Option(..., envvar="POSTGRES_PASSWORD", prompt=True, hide_input=True),
		postgres_db: str = typer.Option(..., envvar="POSTGRES_DB", prompt=True)
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
	with db_connection:
		import_data(data=data, connection=db_connection)

@app.command("latest")
def get_latest(
		time: str = typer.Option(..., prompt=True),
		satelite_id: str = typer.Option(..., prompt=True),
		postgres_host: str = typer.Option(..., envvar="POSTGRES_HOST", prompt=True),
		postgres_port: int = typer.Option(..., envvar="POSTGRES_PORT", prompt=True),
		postgres_user: str = typer.Option(..., envvar="POSTGRES_USER", prompt=True),
		postgres_password: str = typer.Option(..., envvar="POSTGRES_PASSWORD", prompt=True, hide_input=True),
		postgres_db: str = typer.Option(..., envvar="POSTGRES_DB", prompt=True)
	):
	db_connection = get_db_connection(username=postgres_user, password=postgres_password, 
		host=postgres_host, port=postgres_port, db=postgres_db)
	with db_connection:
		cur = db_connection.cursor()
		cur.execute(
			"""
			SELECT satelite_id, last(longitude, recorded_at), last(latitude, recorded_at) 
			FROM satelite_positions 
			WHERE satelite_id = %s AND recorded_at < %s GROUP BY satelite_id;
			""",
			(satelite_id, parser.parse(time).astimezone(tz.gettz("UTC")))
		)
		res = cur.fetchall()
		cur.close()
		typer.echo([desc[0] for desc in cur.description])
		for row in res:
			typer.echo(row)

@app.command("closest")
def get_closest(
		time: str = typer.Option(..., prompt=True),
		longitude: float = typer.Option(..., prompt=True),
		latitude: float = typer.Option(..., prompt=True),
		postgres_host: str = typer.Option(..., envvar="POSTGRES_HOST", prompt=True),
		postgres_port: int = typer.Option(..., envvar="POSTGRES_PORT", prompt=True),
		postgres_user: str = typer.Option(..., envvar="POSTGRES_USER", prompt=True),
		postgres_password: str = typer.Option(..., envvar="POSTGRES_PASSWORD", prompt=True, hide_input=True),
		postgres_db: str = typer.Option(..., envvar="POSTGRES_DB", prompt=True)
	):
	db_connection = get_db_connection(username=postgres_user, password=postgres_password, 
		host=postgres_host, port=postgres_port, db=postgres_db)
	with db_connection:
		cur = db_connection.cursor()
		cur.execute(
			"""
			SELECT points.satelite_id, points.longitude, points.latitude,
				ST_Distance(
					ST_Transform(ST_SetSRID(ST_MakePoint(points.longitude, points.latitude),4326),2163), 
					ST_Transform(ST_SetSRID(ST_MakePoint(%s, %s),4326),2163)
				) 
			FROM (
					SELECT satelite_id, last(longitude, recorded_at) AS longitude, latitude 
					FROM satelite_positions WHERE recorded_at < %s GROUP BY satelite_id, latitude
				) as points ORDER BY
			ST_Distance(
				ST_Transform(ST_SetSRID(ST_MakePoint(points.longitude, points.latitude),4326),2163), 
				ST_Transform(ST_SetSRID(ST_MakePoint(%s,%s),4326),2163)
			) ASC LIMIT 1;
			""",
			(longitude, latitude, parser.parse(time).astimezone(tz.gettz("UTC")), longitude, latitude)
		)
		res = cur.fetchall()
		cur.close()
		typer.echo([desc[0] for desc in cur.description])
		for row in res:
			typer.echo(row)

if __name__ == "__main__":
    app()