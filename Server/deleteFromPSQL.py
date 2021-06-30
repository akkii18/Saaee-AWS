#a well known library for psql
import psycopg2

# define Python user-defined exceptions
class Error(Exception):
    """Base class for other exceptions"""
    pass

class TableNotFoundException(Error):
    """Raising an exception when wrong table name is passed"""
    pass

#getting the connection and returning the connection variable and cursor to the db
def get_connection():
    connection = psycopg2.connect(
        #your endpoint
        host = '[your_db_endpoint]',
        #your psql port on RDS
        port = 5432,
        #master username
        user = '[your_master_username]',
        #master password
        password = '[your_master password]',
        #database INSTANCE name
        database='[your_database_instance_name]'
        )
    cursor=connection.cursor()

    return connection, cursor

#delete from functionality
def delete_from_table(table_name, where_entities, cursor=None):
    #getting the cursor to the DB
    if cursor is None:
        connection, cursor = get_connection()

    where_string = ""
    for key, value in where_entities.items():
        where_string += str(key) + "=" + str(value) + ","

    where_string = where_string[:-1]
        
    #rasining an exception if something goes wrong
    try:
        #executing the query
        query = "DELETE FROM %s WHERE %s" % (table_name, where_string)
        response = cursor.execute(query)
    except Exception as e:
        return str(e)
    
    #committing the changes
    connection.commit()
    
    #return true if deleted successfully
    return True

#function for testing purposes
def test():
    connection, cursor = get_connection()

    cursor.execute("select * from posts")
    return "Select query: " + str(cursor.fetchall())

#entry point function for lambda function
def lambda_handler(event, context):
    #return (test())
    where_entities = event["where_entities"]
    table_name = event["table"]
    
    #checking if the tablename is valid
    try:
        if( table_name != "posts" and table_name != "comments"):
            raise TableNotFoundException
    except TableNotFoundException:
        return "TableNotFoundException: Table not found in DB"

    #executing the update
    flag = delete_from_table(table_name, where_entities)
    
    #returning the message (true/error)
    return (flag)
