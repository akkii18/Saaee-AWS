#a well known library for psql
import psycopg2
import psycopg2.extras
import uuid

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
        password = '[your_master_password]',
        #database INSTANCE name
        database='[your_database_instance_name]'
        )
    cursor=connection.cursor()

    return connection, cursor

#creating tables 
def create_tables(cursor=None):
    if cursor is None:
        connection, cursor = get_connection()
    
    #cursor is the main variable given by connection. It will help us in executing the queries
    #dropping the tables
    cursor.execute("DROP TABLE IF EXISTS comments;")
    cursor.execute("DROP TABLE IF EXISTS posts;")
    cursor.execute("DROP TABLE IF EXISTS messages;")
    cursor.execute("DROP TABLE IF EXISTS conversations;")
    
    cursor.execute("""CREATE TABLE posts(
    id text PRIMARY KEY,
    subject text,
    body text,
    username text,
    timestamp text,
    date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)""")

    cursor.execute("""CREATE TABLE comments(
    id text PRIMARY KEY,
    post_id text,
    body text,
    vote integer,
    username text,
    timestamp text,
    date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_post_id
      FOREIGN KEY(post_id) 
	  REFERENCES posts(id))""")

    cursor.execute("""CREATE TABLE conversations(
    user1 text NOT NULL,
    user2 text NOT NULL,
    conv_id text NOT NULL,
    CONSTRAINT user_convo PRIMARY KEY(user1, user2, conv_id))""")
    
    cursor.execute("""CREATE TABLE messages(
    sender text NOT NULL,
    message text NOT NULL,
    user1 text NOT NULL,
    user2 text NOT NULL,
    conv_id text NOT NULL,
    timestamp text,
    date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_convo_message FOREIGN KEY(user1, user2, conv_id) REFERENCES conversations(user1, user2, conv_id),
    CONSTRAINT pk_composite_message PRIMARY KEY(sender, conv_id, timestamp))""")

    #commiting after any changes
    connection.commit()

    #fetching all the tables in db
    cursor.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';")
    return "Tables in the DB: " + str(cursor.fetchall())

#insert into functionality
def insert_into_table(table_name, values, cursor=None):
    #getting the cursor to the DB
    if cursor is None:
        connection, cursor = get_connection()
        cursor = connection.cursor(cursor_factory = psycopg2.extras.RealDictCursor)

    #generating random id for post/comment id
    id = uuid.uuid4()
    
    #checking the table name
    try:
        if( table_name == "posts"):
            query = "INSERT INTO %s(id, subject, body, username, timestamp) values('%s', '%s', '%s', '%s', '%s')" % (table_name, id, values['subject'], values['body'], values['username'], values['timestamp'])
            
        elif( table_name == "comments"):
            query = "INSERT INTO %s(id, post_id, body, vote, username, timestamp) values('%s', '%s', '%s', %s, '%s', '%s')" % (table_name, id, values['post_id'], values['body'], values['vote'], values['username'], values['timestamp'])
        
        elif( table_name == "conversations" ):
            query = "SELECT user1, user2, conv_id FROM conversations WHERE user2='%s' and user1='%s'" % (values['sender'], values['receiver'])
            result_1 = cursor.execute(query)
            query = "SELECT user1, user2, conv_id FROM conversations WHERE user1='%s' and user2='%s'" % (values['sender'], values['receiver'])
            result_2 = cursor.execute(query)
            
            if(not result_1 and not result_2):
                query = "INSERT INTO %s(user1, user2, conv_id) values('%s', '%s', '%s')" % (table_name, values['sender'], values['receiver'], id)
        
        elif( table_name == "messages" ):
            query = "INSERT INTO %s(sender, message, user1, user2, conv_id, timestamp) values('%s', '%s', '%s', '%s', '%s', '%s')" % (table_name, values['sender'], values['message'], values['user1'], values['user2'], values['conv_id'], values['timestamp'])
        else:
            raise TableNotFoundException
    except TableNotFoundException:
        return "TableNotFoundException: Table not found in DB"
        
    #rasining an exception if something goes wrong
    try:   
        cursor.execute(query)
    except Exception as e:
        return str(e)
    
    #committing the changes
    connection.commit()
    
    #return true if inserted successfully
    return True

#function for testing purposes
def get_data_from_db(table_name, where_entities=None):
    connection, cursor = get_connection()
    cursor = connection.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
    
    #creating the where string for psql from where_entities
    if( where_entities != None):
        where_string = ""
        for key, value in where_entities.items():
            if( isinstance(value, str) ):
                where_string += str(key) + "=" + "'" + str(value) + "' " +"AND "
            else:
                where_string += str(key) + "=" + str(value) +" AND "
        
        where_string = where_string[:-4]
        
        if( table_name == "posts" ):
            query = "SELECT id, subject, body, username, timestamp FROM %s WHERE %s ORDER BY date DESC" % (table_name, where_string)
        elif( table_name == "comments" ):
            query = "SELECT id, post_id, body, vote, username, timestamp FROM %s WHERE %s ORDER BY date DESC" % (table_name, where_string)
        elif( table_name == "conversations" ):
            query = "SELECT conv_id FROM %s WHERE %s" % (table_name, where_string)
        else:
            conv = get_data_from_db("conversations", where_entities)
            if(len(conv) != 0):
                conv_id = conv[0]['conv_id']
                query = "SELECT sender, message, user1, user2, timestamp FROM %s WHERE conv_id = '%s' ORDER BY date DESC" % (table_name, conv_id)
            else:
                return False
                
    else:
        if(table_name == "posts" ):
            query = "SELECT id, subject, body, username, timestamp FROM %s ORDER BY date DESC" % (table_name)
        else:
            query = "SELECT id, post_id, body, vote, username, timestamp FROM %s ORDER BY date DESC" % (table_name)
    
    cursor.execute(query)
    return cursor.fetchall()

#entry point function for lambda function
def lambda_handler(event, context):
    #getting the values from the events
    values = event["values"]
    table_name = event["table"]
    operation = event["operation"]
    where_entities = event["where_entities"]
    
    #if the operation is get i.e. get data then call get data function else insert function
    if(operation == "get"):
        if( where_entities == "" ):
            return get_data_from_db(table_name, None)
        else:
            return get_data_from_db(table_name, where_entities)
    else:
        flag = insert_into_table(table_name, values)
        return flag

