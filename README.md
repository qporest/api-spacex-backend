# Notes, choices
I chose to use TimescaleDB as my database of choice. One of the reasons is the built in functions for time series support, and also it comes with PostGIS. It seemes like a perfect choice since it natively support time and geo queries.

Script assumes that the schema is correct, and the actual tables are created by an `.sql` script.
When planning for a bigger project with more time budget, the better approach would be to use `alembic` for migrations.

To speed up the upload of data I've used `pgcopy` which uses COPY mechanism that PosgtreSQL allows.
I've shot myself in the foot a little bit, by creating a second table with foreign key constrains. Something like this would probably exist in a true project, but in this case it made `pgcopy` a slightly unfortunate choice, since now the data had to be processed twice
- To create the entries for satelities
- Only then to create the time series data

But, it doesn't handle conflicts, unlike what `INSERT .. ON CONFLICT DO NOTHING` could do. That would be a good change if the script had to add data to existing database. For now it requires `docker-compose down` and `docker-compose up -d` again to clear the db :)

For script I've used Typer (built on type of Click), that allows to create a nice interface and even completions for the command line.
It allows to specify the environmental variables where the arguments can come from, as well as prompt used for the values, and validate the type.

# Future impovements to this
`alembic` for schema management and migration, `INSERT` for uploading data to an existing table. `CI/CD`, testing

Maybe, for finding the closest satelite having a cut off period would be good, because at the moment it find which satelite had its latest position recorded near given point in space, but it might be the last reading of a satelite from 50 years ago.

# Usage
`pip install -r requirements.txt` to install all dependencies. Since it's not packaged with setup.py they have to be installed separately.

Defaults:
- host: localhost
- port: 5432
- username: postgres
- password: test
- database: spacex

`docker-compose up -d` to start the database. In docker-compose file you can define the password used.
`python spacex_api.py --help` for the list of commands.
## Task 2
`python spacex_api.py import --file starlink_historical_data.json` To import the file. Additional prompts will appear for connection parameters.
Or `env POSTGRES_USER=postgres POSTGRES_PASSWORD=test POSTGRES_DB=spacex POSTGRES_HOST=localhost POSTGRES_PORT=5432 python spacex_api.py import --file starlink_historical_data.json` to have the values read from environment.
Or `cat starlink_historical_data.json | env env POSTGRES_USER=postgres POSTGRES_PASSWORD=test POSTGRES_DB=spacex POSTGRES_HOST=localhost POSTGRES_PORT=5432 python spacex_api.py import --stdin` to get the data from stdin. Can be used to pipe with curl

<details>
  <summary>Example</summary>
  ```
  ▶ env POSTGRES_USER=postgres POSTGRES_PASSWORD=test POSTGRES_DB=spacex POSTGRES_HOST=localhost POSTGRES_PORT=5432 python spacex_api.py import --file starlink_historical_data.json
  Success.
  ```
</details>

## Task 3
`python spacex_api.py latest` to find latest position. All previous options also apply (not stdin)

<details>
  <summary>Example</summary>
  ```
  ▶ env POSTGRES_USER=postgres POSTGRES_PASSWORD=test POSTGRES_DB=spacex POSTGRES_HOST=localhost POSTGRES_PORT=5432 python spacex_api.py latest
  Time: 2021-01-26T06:26:20
  Satelite id: 5eed7714096e590006985664
  ['satelite_id', 'last', 'last']
  ('5eed7714096e590006985664', Decimal('52'), Decimal('53.120406589983275580379995517432689666748046875'))
  ```
</details>


## Task 4
`python spacex_api.py closest` to find closet satelite at a given time.

<details>
  <summary>Example</summary>
  ```
  ▶ env POSTGRES_USER=postgres POSTGRES_PASSWORD=test POSTGRES_DB=spacex POSTGRES_HOST=localhost POSTGRES_PORT=5432 python spacex_api.py closest
  Time: 2021-01-26T06:26:20
  Longitude: 52
  Latitude: 53
  ['satelite_id', 'longitude', 'latitude', 'st_distance']
  ('5eed7714096e590006985664', Decimal('52'), Decimal('53.120406589983275580379995517432689666748046875'), 11353.569371288984)
  ```
</details>

