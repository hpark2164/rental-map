import mysql.connector
from mysql.connector import Error
import keyring

'''
Retrieve the stored password for the MySQL database root user
from MacOS Keychain Manager
'''
def get_keychain_password():
    return keyring.get_password("Mysql@127.0.0.1:3306", "root")

'''
Connect to the local MySQL database
'''
def create_server_connection():
    connection = None
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            passwd=get_keychain_password(),
            database="rental_schema"
        )
        print("Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection
