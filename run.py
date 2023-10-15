import boto3
import os

region = 'us-east-1'
aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')


def create_table(ddb, table_name, key_attribute, read_capacity, write_capacity):
    return ddb.create_table(
        TableName=table_name,
        KeySchema=[{'AttributeName': key_attribute, 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': key_attribute, 'AttributeType': 'S'}],
        ProvisionedThroughput={'ReadCapacityUnits': read_capacity, 'WriteCapacityUnits': write_capacity}
    )

def create_gsi(ddb, table_name, index_name, key_attribute):
    table = ddb.Table(table_name)
    return table.update(
        AttributeDefinitions=[{'AttributeName': key_attribute, 'AttributeType': 'S'}],
        GlobalSecondaryIndexUpdates=[
            {
                'Create': {
                    'IndexName': index_name,
                    'KeySchema': [{'AttributeName': key_attribute, 'KeyType': 'HASH'}],
                    'Projection': {'ProjectionType': 'ALL'},
                    'ProvisionedThroughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5},
                },
            },
        ],
    )

def insert_data(ddb, table_name, data):
    table = ddb.Table(table_name)
    for item in data:
        # Create a new item without 'tag_id'
        item_to_insert = {key: value for key, value in item.items() if key != 'tag_id'}
        table.put_item(Item=item_to_insert)


def check_and_add_tag(ddb, table_name, key_attribute, tag_id):
    table = ddb.Table(table_name)
    response = table.get_item(Key={key_attribute: tag_id})
    if 'Item' not in response:
        table.put_item(Item={key_attribute: tag_id})

def show_all_tables(ddb):
    return [table.name for table in ddb.tables.all()]

def show_all_items(ddb, table_name):
    table = ddb.Table(table_name)
    items = table.scan()['Items']
    for item in items:
        print(item)

def delete_table(ddb, table_name):
    table = ddb.Table(table_name)
    if table.name in show_all_tables(ddb):
        table.delete()
        table.wait_until_not_exists()
        print(f"Deleted table: {table_name}")
    else:
        print(f"Table {table_name} does not exist")

def main():
    ddb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000', region_name=region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    delete_table(ddb, 'Article')
    delete_table(ddb, 'Tag')
    delete_table(ddb, 'Account')
    delete_table(ddb, 'Appointment')
    
    create_table(ddb, 'Article', 'article_id', 10, 10)
    create_table(ddb, 'Tag', 'tag_id', 10, 10)
    create_table(ddb, 'Account', 'account_id', 10, 10)
    create_table(ddb, 'Appointment', 'appointment_id', 10, 10)
    
    # create_gsi(ddb, 'Article', 'TagIndex', 'tag_id')
    insert_data(ddb, 'Article', [{'tag_id': 'tag1', 'article_id': '1', 'author': 'Author 1', 'title': 'Article 1', 'last_edit': '12-12-1950', 'content': 'So easy!!!'},
                                {'tag_id': 'tag1', 'article_id': '2', 'author': 'Author 2', 'title': 'Article 2', 'last_edit': '12-12-1950', 'content': 'So easy!!!'},
                                {'tag_id': 'tag2', 'article_id': '3', 'author': 'Author 3', 'title': 'Article 3', 'last_edit': '12-12-1950', 'content': 'So easy!!!'}])
    insert_data(ddb, 'Account', [{'account_id': '1', 'username': 'doctor1', 'user_type': 'Doctor', 'working_time': '9 AM - 5 PM'},
                              {'account_id': '2', 'username': 'admin1', 'user_type': 'Admin', 'admin_data': 'Admin-specific data'},
                              {'account_id': '3', 'username': 'user1', 'user_type': 'User', 'user_data': 'User-specific data'}])
    insert_data(ddb, 'Account', [{'user_id': '1', 'username': 'doctor1', 'user_type': 'Doctor', 'working_time': '9 AM - 5 PM'},
                              {'account_id': '2', 'username': 'admin1', 'user_type': 'Admin', 'admin_data': 'Admin-specific data'},
                              {'account_id': '3', 'username': 'user1', 'user_type': 'User', 'user_data': 'User-specific data'}])
    
    show_all_tables(ddb)
    show_all_items(ddb, 'Article')
    show_all_items(ddb, 'Tag')
    show_all_items(ddb, 'Account')

if __name__ == "__main__":
    main()
