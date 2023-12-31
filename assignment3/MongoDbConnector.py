from pymongo import MongoClient, version
from pymongo.database import Database


class DbConnector:
    """
    Connects to the MongoDB server on the Ubuntu virtual machine.
    Connector needs HOST, USER and PASSWORD to connect.

    Example:
    HOST = "tdt4225-00.idi.ntnu.no" // Your server IP address/domain name
    USER = "testuser" // This is the user you created and added privileges for
    PASSWORD = "test123" // The password you set for said user
    """

    db: Database

    def __init__(
        self,
        DATABASE="activity_tracker",
        HOST="localhost",  # "tdt4225-xx.idi.ntnu.no",
        USER="group6",
        PASSWORD="test123",
    ):
        uri = "mongodb://%s:%s@%s/%s" % (USER, PASSWORD, HOST, DATABASE)
        # Connect to the databases
        try:
            self.client = MongoClient(uri)
            self.db = self.client[DATABASE]
        except Exception as e:
            print("ERROR: Failed to connect to db:", e)

    def close_connection(self):
        # close the cursor
        # close the DB connection
        self.client.close()
