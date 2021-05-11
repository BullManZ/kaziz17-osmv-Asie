import boto3

from src.library.dynamo.Client import Client
from src.library.dynamo.Table import Table


class Osmv(object):

    envs_table_name = "envs"

    def __init__(self, is_local, bucket_name):
        self.is_local = is_local
        self.bucket_name = bucket_name

    def init_aws_resources(self):
        # Get the service resource.
        if self.is_local:
            dbr = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')
            dbc = boto3.client('dynamodb', endpoint_url='http://localhost:8000')

        else:

            dbr = boto3.resource('dynamodb')
            dbc = boto3.client('dynamodb')

        s3r = boto3.resource('s3')
        s3c = boto3.client('s3')

        bucket = s3r.Bucket(self.bucket_name)

        return dbr, dbc, s3r, s3c, bucket

    def create_env(self, env_name, db_dict, db_schema):
        (dbr, dbc, s3r, s3c, bucket) = self.init_aws_resources()
        DB = Client(dbc)
        DB.create_table(self.envs_table_name, "name", "S")
        table_envs = Table(dbr.Table(self.envs_table_name))
        env_item = {"name": env_name, "db_dict": db_dict}
        table_envs.put_item(env_item)
        for schema in db_schema.values():
            DB.create_table(schema[0], schema[1], schema[2], schema[3], schema[4], schema[5])
        #DB.add_index("data", "reverse", "trade_date", "S", "ref", "S")
        # DB.add_index("ticker_expis", "env-expi-index", "env", "S", "expi", "S")
        # DB.add_index("ticker_expis", "env-ticker-index", "env", "S", "ticker", "S")
        return dbr, dbc, s3r, s3c, bucket, db_dict

    def select_env(self, env_name):
        (dbr, dbc, s3r, s3c, bucket) = self.init_aws_resources()
        DB = Client(dbc)
        tables = DB.list_tables()
        if "envs" not in tables:
            raise ValueError("You need to create envs table")
        table_envs = Table(dbr.Table(self.envs_table_name))
        existing_envs = table_envs.scan()
        names = [ e["name"] for e in existing_envs]
        if env_name in names:
            env = table_envs.get_item("name",env_name)
            db_dict = env["db_dict"]
            return dbr, dbc, s3r, s3c, bucket, db_dict
        else:
            raise ValueError("You need to create this env")

    def add_table(self, env_name, table_key, table_name, hash_name, hash_type='S', has_range=False, range_name=None, range_type='S'):
        (dbr, dbc, s3r, s3c, bucket) = self.init_aws_resources()
        DB = Client(dbc)
        tables = DB.list_tables()
        if "envs" not in tables:
            raise ValueError("You need to create envs table")
        table_envs = Table(dbr.Table(self.envs_table_name))
        existing_envs = table_envs.scan()
        names = [ e["name"] for e in existing_envs]
        if env_name in names:
            env = table_envs.get_item("name",env_name)
            db_dict = env["db_dict"]
            if table_name in db_dict.values():
                raise ValueError("This table exists already")
            else:
                try:
                    DB.create_table(table_name, hash_name, hash_type, has_range, range_name, range_type)
                except:
                    print("unable to create table")
                db_dict[table_key]=table_name
            env["db_dict"] = db_dict
            table_envs.put_item(env)

        else:
            raise ValueError("This env does not exist")

