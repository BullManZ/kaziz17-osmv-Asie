from src.library.params import param

class Client(object):

    def __init__(self, db_client):
        self.db_client = db_client

    def create_table(self, name, hash_name, hash_type='S', has_range=False, range_name=None, range_type='S'):
        existing_table_names = self.list_tables()
        if name not in existing_table_names:

            if has_range:
                key_schema = [
                    {
                        'AttributeName': hash_name,
                        'KeyType': 'HASH'
                    },
                    {
                        'AttributeName': range_name,
                        'KeyType': 'RANGE'
                    }
                ]
            else:
                key_schema = [
                    {
                        'AttributeName': hash_name,
                        'KeyType': 'HASH'
                    }
                ]
            if has_range:
                att_def = [
                    {
                        'AttributeName': hash_name,
                        'AttributeType': hash_type,
                    },
                    {
                        'AttributeName': range_name,
                        'AttributeType': range_type,
                    }
                ]
            else:
                att_def = [
                    {
                        'AttributeName': hash_name,
                        'AttributeType': hash_type
                    }
                ]

            self.db_client.create_table(
                TableName=name,
                KeySchema=key_schema,
                AttributeDefinitions=att_def,
                BillingMode='PAY_PER_REQUEST',
            )
            # Wait until the table exists.
            self.db_client.get_waiter('table_exists').wait(TableName=name)
            return name + " OK"
        else:
            return "Table " + name + " exists already"

    def add_index(self, table_name, index_name, hash_name, hash_type, range_name, range_type):
        response = self.db_client.update_table(
            AttributeDefinitions=[
                {
                    'AttributeName': hash_name,
                    'AttributeType': hash_type
                },
                {
                    'AttributeName': range_name,
                    'AttributeType': range_type
                },
            ],
            TableName=table_name,
            BillingMode='PAY_PER_REQUEST',
            GlobalSecondaryIndexUpdates=[
                {
                    'Create': {
                        'IndexName': index_name,
                        'KeySchema': [
                            {
                                'AttributeName': hash_name,
                                'KeyType': 'HASH'
                            },
                            {
                                'AttributeName': range_name,
                                'KeyType': 'RANGE'
                            }
                        ],
                        'Projection': {
                            'ProjectionType': 'ALL'
                        },
                    }
                }
            ]
        )
        return response

    def list_tables(self):
        return self.db_client.list_tables()["TableNames"]

    def delete_all_tables(self):
        for tb in param.db_dict.values():
            response = self.db_client.delete_table(
                TableName=tb
            )
