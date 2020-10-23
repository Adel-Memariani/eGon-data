import subprocess
import os
from urllib.request import urlretrieve
import yaml
import egon.data
from egon.data import utils
import time
import json


def download_osm_file():
    data_config = utils.data_set_configuration()
    osm_config = data_config["openstreetmap"]["original_data"]["osm"]

    if not os.path.isfile(osm_config["file"]):
        urlretrieve(osm_config["url"] + osm_config["file"], osm_config["file"])


def osm2postgres(osm_file=None, num_processes=4, cache_size=4096):

    # Read database configuration from docker-compose.yml
    docker_db_config = utils.egon_data_db_credentials()

    # Get data set config
    data_config = utils.data_set_configuration()
    osm_config = data_config["openstreetmap"]["original_data"]["osm"]

    # Prepare osm2pgsql command
    cmd = ["osm2pgsql",
           "--create",
           "--slim",
           "--hstore-all",
           f"--number-processes {num_processes}",
           f"--cache {cache_size}",
           f"-H {docker_db_config['HOST']} -P {docker_db_config['PORT']} "
           f"-d {docker_db_config['POSTGRES_DB']} -U {docker_db_config['POSTGRES_USER']}",
           f"-p {osm_config['table_prefix']}",
           f"-S {osm_config['stylefile']}",
           f"{osm_config['file']}"
           ]

    # Execute osm2pgsql for import OSM data
    subprocess.run(" ".join(cmd),
                   shell=True,
                   env={"PGPASSWORD": docker_db_config['POSTGRES_PASSWORD']},
                   cwd=os.path.dirname(__file__))


def post_import_modifications():

    # Replace indices and primary keys
    for table in ["osm_" + suffix for suffix in ["line", "point", "polygon", "roads"]]:

        # Drop indices
        sql_statements = [f"DROP INDEX {table}_index;"]

        # Drop primary keys
        sql_statements.append(f"DROP INDEX {table}_pkey;")

        # Add primary key on newly created column "gid"
        sql_statements.append(f"ALTER TABLE public.{table} ADD gid SERIAL;")
        sql_statements.append(f"ALTER TABLE public.{table} ADD PRIMARY KEY (gid);")
        sql_statements.append(f"ALTER TABLE public.{table} RENAME COLUMN way TO geom;")

        # Add indices (GIST and GIN)
        sql_statements.append(f"CREATE INDEX {table}_geom_idx ON public.{table} USING gist (geom);")
        sql_statements.append(f"CREATE INDEX {table}_tags_idx ON public.{table} USING GIN (tags);")

        # Execute collected SQL statements
        for statement in sql_statements:
            utils.execute_sql(statement)

    # Get data set config
    data_config = utils.data_set_configuration()["openstreetmap"]["original_data"]["osm"]

    # Move table to schema "openstreetmap"
    utils.execute_sql(f"CREATE SCHEMA IF NOT EXISTS {data_config['output_schema']};")

    for out_table in data_config['output_tables']:
        sql_statement = f"ALTER TABLE public.{out_table} SET SCHEMA {data_config['output_schema']};"

        utils.execute_sql(sql_statement)


def metadata():

    # Prepare variables
    osm_config = utils.data_set_configuration()["openstreetmap"]["original_data"]["osm"]
    spatial_and_date = os.path.basename(osm_config["file"]).split("-")
    spatial_extend = spatial_and_date[0]
    osm_data_date = "20" + spatial_and_date[1][0:2] + "-" + spatial_and_date[1][2:4] + "-" + spatial_and_date[1][4:6]
    osm_url = osm_config["url"]

    # Insert metadata for each table
    for table in osm_config['output_tables']:
        table_suffix = table.split("_")[1]
        meta = {
            "title": f"OpenStreetMap (OSM) - Germany - {table_suffix}",
            "description": "OpenStreetMap is a free, editable map of the whole world that is "
                           "being built by volunteers largely from scratch and released with "
                           "an open-content license.",
            "language": ["EN", "DE"],
            "spatial": {
                "location": "",
                "extent": f"{spatial_extend}",
                "resolution": ""
            },
            "temporal": {
                "referenceDate": f"{osm_data_date}",
                "timeseries": {
                    "start": "",
                    "end": "",
                    "resolution": "",
                    "alignment": "",
                    "aggregationType": ""
                }
            },
            "sources": [
                {
                    "title": "Geofabrik - Download - OpenStreetMap Data Extracts",
                    "description": "Data dump of reference date. Thereof, a subset is selected using"
                                   "osm2pgsql with oedb.style style file",
                    "path": f"{osm_url}",
                    "licenses": [
                        {
                            "name": "Open Data Commons Open Database License 1.0",
                            "title": "",
                            "path": "https://opendatacommons.org/licenses/odbl/1.0/",
                            "instruction": "You are free: To Share, To Create, To Adapt; As long as you: Attribute, Share-Alike, Keep open!",
                            "attribution": "© Reiner Lemoine Institut"
                        }
                    ]
                }
            ],
            "licenses": [
                {
                    "name": "Open Data Commons Open Database License 1.0",
                    "title": "",
                    "path": "https://opendatacommons.org/licenses/odbl/1.0/",
                    "instruction": "You are free: To Share, To Create, To Adapt; As long as you: Attribute, Share-Alike, Keep open!",
                    "attribution": "© Reiner Lemoine Institut"
                }
            ],
            "contributors": [
                {
                    "title": "Guido Pleßmann",
                    "email": "http://github.com/gplssm",
                    "date": time.strftime("%Y-%m-%d"),
                    "object": "",
                    "comment": "Imported data"
                }
            ],
            "metaMetadata": {
                "metadataVersion": "OEP-1.4.0",
                "metadataLicense": {
                    "name": "CC0-1.0",
                    "title": "Creative Commons Zero v1.0 Universal",
                    "path": "https://creativecommons.org/publicdomain/zero/1.0/"
                }
            },
        }

        meta_json = "'" + json.dumps(meta) + "'"

        utils.submit_comment(meta_json, "openstreetmap", table)

# TODO: rename "data_import" to "import"
# TODO: write docstrings for added functions
# TODO: add module docstring
# TODO: report in CHANGELOG



