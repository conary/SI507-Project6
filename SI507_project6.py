# Import statements
import psycopg2
import psycopg2.extras
from psycopg2 import sql
import requests
from config import *
import sys
import json
from csv import DictReader






#tweet_diction["user"]["screen_name"]









# state_dict={'name':'California'}
# insert(db_connection, db_cursor, "States", state_dict)









# Write code / functions to create tables with the columns you want and all database setup here.

db_connection, db_cursor = None, None

# Write code / functions to set up database connection and cursor here.
def get_connection_and_cursor():
    global db_connection, db_cursor
    if not db_connection:
        try:
            db_connection = psycopg2.connect("dbname='{0}' user='{1}' password='{2}'".format(db_name, db_user, db_password))
            print("Success connecting to database")
        except:
            print("Unable to connect to the database. Check server and credentials.")
            sys.exit(1) # Stop running program if there's no db connection.

    if not db_cursor:
        db_cursor = db_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    return db_connection, db_cursor

def setup_database():
    # Invovles DDL commands
    # DDL --> Data Definition Language
    # CREATE, DROP, ALTER, RENAME, TRUNCATE
    
    conn, db_cursor = get_connection_and_cursor()
    db_cursor.execute("DROP TABLE IF EXISTS Sites")
    db_cursor.execute("DROP TABLE IF EXISTS States")
    
    db_cursor.execute("""CREATE TABLE IF NOT EXISTS States(
        ID SERIAL PRIMARY KEY,
        name VARCHAR(40) UNIQUE
        )""")


    db_cursor.execute("""CREATE TABLE IF NOT EXISTS Sites(
        ID SERIAL PRIMARY KEY,
        Name VARCHAR(128) UNIQUE,
        Type VARCHAR(128) NOT NULL,
        State_ID INTEGER REFERENCES States(ID),
        Location VARCHAR(255),
        Description TEXT
        )""")


    # db_cursor.execute(INSERT INTO  States(Name) VALUES(%S)) RETURNING ID
    # result = db_cursor.fetchone()
    # print(result)
    
    conn.commit()


# Write code / functions to deal with CSV files and insert data into the database here.

def insert_into_db(conn, cur, table, data_dict, no_return=False):
    """Accepts connection and cursor, table name, dictionary that represents one row, and inserts data into table. (Not the only way to do this!)"""
    column_names = data_dict.keys()
    print(column_names, "column_names") # for debug
    if not no_return:
        query = sql.SQL('INSERT INTO {0}({1}) VALUES({2}) ON CONFLICT DO NOTHING RETURNING id').format(
            sql.SQL(table),
            sql.SQL(', ').join(map(sql.Identifier, column_names)),
            sql.SQL(', ').join(map(sql.Placeholder, column_names))
        )
    else:
        query = sql.SQL('INSERT INTO {0}({1}) VALUES({2}) ON CONFLICT DO NOTHING').format(
            sql.SQL(table),
            sql.SQL(', ').join(map(sql.Identifier, column_names)),
            sql.SQL(', ').join(map(sql.Placeholder, column_names))
        )
    query_string = query.as_string(conn) # thanks to sql module
    cur.execute(query_string, data_dict) # will mean that id is in cursor, because insert statement returns id in this function
    if not no_return:
        return cur.fetchone()['id']





  
  
  



# Make sure to commit your database changes with .commit() on the database connection.



# Write code to be invoked here (e.g. invoking any functions you wrote above)

get_connection_and_cursor();
setup_database();


# state_dict={'name':'Arkansas'}
# insert(db_connection, db_cursor, "States", state_dict)
state_dict={'name':'Michigan'}
print(state_dict)
print (type(state_dict))
state_id_mi = insert_into_db(db_connection, db_cursor, "States", state_dict)
#print(state_id_mi)
insert_dict = {}
the_reader = DictReader(open('michigan.csv', 'r'))
for line_dict in the_reader:
    #print(line_dict)
    #print (type(line_dict))
    line_dict['State_ID'] = state_id_mi
    insert_dict['name'] = line_dict['NAME']
    insert_dict['type'] = line_dict['TYPE']
    insert_dict['state_id'] = line_dict['State_ID']
    insert_dict['location'] = line_dict['LOCATION']
    insert_dict['description'] = line_dict['DESCRIPTION']
    print(insert_dict)
    insert_into_db(db_connection, db_cursor, "Sites", insert_dict, True)  

db_connection.commit()


# Write code to make queries and save data in variables here.






# We have not provided any tests, but you could write your own in this file or another file, if you want.
