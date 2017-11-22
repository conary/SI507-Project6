# Import statements
import psycopg2
import psycopg2.extras
from psycopg2 import sql
import requests
from config import *
import sys
import json


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
    cur.execute("DROP TABLE IF EXISTS Sites")
    cur.execute("DROP TABLE IF EXISTS States")
    conn, cur = get_connection_and_cursor()
    
    cur.execute("""CREATE TABLE IF NOT EXISTS States(
        ID INTEGER,
        name VARCHAR(40) UNIQUE
        )""")


    cur.execute("""CREATE TABLE IF NOT EXISTS Sites(
        ID SERIAL PRIMARY KEY,
        Type VARCHAR(128) NOT NULL,
        State_ID INTEGER REFERENCES States(ID),
        location VARCHAR(255),
        Description (TEXT)
        )""")


    
    conn.commit()


setup_database();

# Write code / functions to create tables with the columns you want and all database setup here.



# Write code / functions to deal with CSV files and insert data into the database here.



# Make sure to commit your database changes with .commit() on the database connection.



# Write code to be invoked here (e.g. invoking any functions you wrote above)



# Write code to make queries and save data in variables here.






# We have not provided any tests, but you could write your own in this file or another file, if you want.
