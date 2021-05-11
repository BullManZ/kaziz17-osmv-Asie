from boto3.dynamodb.conditions import Key

from src.library.helpers.general import dict_float_to_decimal
from src.library.helpers.general import null


class Table(object):

    def __init__(self, db_table):
        self.table = db_table

    def put_item(self, item):

        response = self.table.put_item(
            Item=item
        )
        return response

    def get_item(self, hash_name, hash_key, has_range=False, range_name=None, range_key=None):
        if has_range:
            key_dict = {hash_name: hash_key, range_name: range_key}
        else:
            key_dict = {hash_name: hash_key}
        try:
            response = self.table.get_item(Key=key_dict)
            resp_dict = response["Item"]
            return resp_dict
        except:
            return None

    def put_df_batch(self, df):

        df_range = range(len(df))
        with self.table.batch_writer() as batch:
            for i in df_range:
                base_dict = df.iloc[i].to_dict()
                dyn_dict = dict_float_to_decimal({k: v for (k, v) in base_dict.items() if not null(v)})
                batch.put_item(
                    Item=dyn_dict
                )

    def query(self, hash_name, hash_key):
        total_items = []
        response = self.table.query(
            KeyConditionExpression=Key(hash_name).eq(hash_key)
        )
        total_items.extend(response['Items'])

        while "LastEvaluatedKey" in response:
            response = self.table.query(
                KeyConditionExpression=Key(hash_name).eq(hash_key),
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            total_items.extend(response['Items'])

        return total_items

    def query_index(self, index_name, hash_name, hash_key, has_range=False, range_name=None, range_key=None):
        total_items = []
        if has_range:
            KCE = Key(hash_name).eq(hash_key) & Key(range_name).eq(range_key)
        else:
            KCE = Key(hash_name).eq(hash_key)

        response = self.table.query(
            IndexName=index_name,
            KeyConditionExpression=KCE
        )
        total_items.extend(response['Items'])

        while "LastEvaluatedKey" in response:
            response = self.table.query(
                IndexName=index_name,
                KeyConditionExpression=KCE,
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            total_items.extend(response['Items'])

        return total_items

    def query_index_range_begins_with(self, index_name, hash_name, hash_key, range_name, range_prefix):
        total_items = []
        KCE = Key(hash_name).eq(hash_key) & Key(range_name).begins_with(range_prefix)

        response = self.table.query(
            IndexName=index_name,
            KeyConditionExpression=KCE
        )
        total_items.extend(response['Items'])

        while "LastEvaluatedKey" in response:
            response = self.table.query(
                IndexName=index_name,
                KeyConditionExpression=KCE,
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            total_items.extend(response['Items'])

        return total_items

    def query_range_gt_than(self, hash_name, hash_key, range_name, range_min):
        total_items = []
        KCE = Key(hash_name).eq(hash_key) & Key(range_name).gt(range_min)

        response = self.table.query(

            KeyConditionExpression=KCE
        )
        total_items.extend(response['Items'])

        while "LastEvaluatedKey" in response:
            response = self.table.query(
                KeyConditionExpression=KCE,
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            total_items.extend(response['Items'])

        return total_items

    def query_range_between(self, hash_name, hash_key, range_name, range_min, range_max):
        total_items = []
        KCE = Key(hash_name).eq(hash_key) & Key(range_name).between(range_min, range_max)

        response = self.table.query(

            KeyConditionExpression=KCE
        )
        total_items.extend(response['Items'])

        while "LastEvaluatedKey" in response:
            response = self.table.query(
                KeyConditionExpression=KCE,
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            total_items.extend(response['Items'])

        return total_items

    def query_index_between(self, index_name, hash_name, hash_list):
        items = []
        for h in hash_list:
            items.extend(self.query_index(index_name, hash_name, h))
        return items

    def scan(self):
        total_items = []
        response = self.table.scan()
        total_items.extend(response['Items'])

        while "LastEvaluatedKey" in response:
            response = self.table.scan(
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            total_items.extend(response['Items'])
        return total_items

    def truncate(self, hash_key, has_range=False, range_key=None):
        if has_range:
            proj = '#' + hash_key + ', ' + '#' + range_key
            expr = {'#' + hash_key: hash_key, '#' + range_key: range_key}
        else:
            proj = '#' + hash_key
            expr = {'#' + hash_key: hash_key}

        response = self.table.scan(
            ProjectionExpression=proj,
            ExpressionAttributeNames=expr
        )
        data = response.get('Items')

        while 'LastEvaluatedKey' in response:
            response = self.table.scan(ProjectionExpression=proj,
                                       ExpressionAttributeNames=expr,
                                       ExclusiveStartKey=response['LastEvaluatedKey'])
            data.extend(response['Items'])

        with self.table.batch_writer() as batch:
            for each in response['Items']:
                batch.delete_item(Key=each)
