import boto3
from boto3.dynamodb.conditions import Key, Attr
# from cryptography.fernet import Fernet

#specifying global variables - region name, access key and secret access key
REGION_NAME="[your_region]"
AWS_ACCESS_KEY_ID="[your_AWS_ACCESS_KEY_ID]"
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

def validate_login(table_name, values, dynamodb=None):
    if dynamodb is None:
        dynamodb = get_client('dynamodb')

    table = dynamodb.Table(table_name)
    #checking if the music is already subscribed
    try:
        response = table.scan(
            Select = 'ALL_ATTRIBUTES',
            FilterExpression = Attr('username').eq(values['username']) & Attr('password').eq(values['password'])
        )

        #if not then add the music subscription
        if not response['Items']:
            return False
        else:
            if 'info' not in response['Items'][0]:
                return "display_questionnare"
            else:
                return True
    except Exception as e:
        return str(e)

def lambda_handler(event, context):
    #help("modules")
    #return "hello_world"
    table_name = event['table']
    values = event['values']

    flag = validate_login(table_name, values)

    return flag
    
    
