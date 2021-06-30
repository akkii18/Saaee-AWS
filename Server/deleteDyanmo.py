import boto3

#specifying global variables - region name, access key and secret access key
REGION_NAME="[your_db_endpoint]"
AWS_ACCESS_KEY_ID="[your_ACCESS_KEY_ID]"
AWS_SECRET_ACCESS_KEY= "[your_SECRET_ACCESS_KEY]"

# define Python user-defined exceptions
class Error(Exception):
    """Base class for other exceptions"""
    pass

class TableNotFoundException(Error):
    """Raising an exception when wrong table name is passed"""
    pass

#function to get a client from AWS - e.g. s3, dynamodb etc.
def get_client(client_name):
    client = boto3.resource(client_name, endpoint_url="https://" + client_name + "." + REGION_NAME + ".amazonaws.com",
        region_name=REGION_NAME,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key = AWS_SECRET_ACCESS_KEY)
    
    return client

def delete_from_table(table_name, where_entities, dynamodb=None):
    if dynamodb is None:
        dynamodb = get_client('dynamodb')
    
    #accessing the table from dynamodb
    table = dynamodb.Table(table_name)

    #putting the user into the table
    response = table.delete_item(
        Key = where_entities
    )

    return response
    
def lambda_handler(event, context):
    table_name = event['table']
    where_entities = event['where_entities']

    flag = delete_from_table(table_name, where_entities)

    return flag
