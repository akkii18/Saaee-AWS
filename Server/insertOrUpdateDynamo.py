import boto3
from boto3.dynamodb.conditions import Key, Attr
#specifying global variables - region name, access key and secret access key
REGION_NAME="[your_region]"
AWS_ACCESS_KEY_ID="[your_AWS_KEY_ID]"
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

def insert_into_table(table_name, values, operation, dynamodb=None):
    if dynamodb is None:
        dynamodb = get_client('dynamodb')
    
    #accessing the table from dynamodb
    table = dynamodb.Table(table_name)

    if(table_name == "UserDetails"):
        try:
            response = table.scan(
                Select = 'ALL_ATTRIBUTES',
                FilterExpression = Attr('username').eq(values['username'])
            )
            if( operation == "get"):
                if not response['Items']:
                    return False
                else:
                    return response['Items']
                    
            if( operation == "update"):
                if not response['Items']:
                    return False
                else:
                    response = table.put_item(
                        Item=values
                    )
                        
                    return True
                    
            if( operation == "insert"):
                if not response['Items']:
                    response = table.put_item(
                        Item=values
                    )
                        
                    return True
                else:
                    return False
        except Exception as e:
            return str(e)
    else:
        response = table.put_item(
                Item=values
            )
                
        return True

    if( operation == "update"):
        try:
            response = table.put_item(
                    Item=values
                )
                    
            return True
        except Exception as e:
            return str(e)
        
    
    
    
def lambda_handler(event, context):
    table_name = event['table']
    values = event['values']    
    operation = event['operation']
    
    flag = insert_into_table(table_name, values, operation)

    return flag




