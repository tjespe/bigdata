from DbConnector import DbConnector
from helpers import printtable

# Find the number of users which have been close to each other in time and space.
# Close is defined as the same space (50 meters) and for the same half minute (30 seconds)

def s8_users_close_in_time_and_space():
    connection = DbConnector()
    db_connection = connection.db_connection
    cursor = connection.cursor

    query = """
            

            """
    cursor.execute(query)
    coloumns = []
    data = cursor.fetchall()
    # can also use cursor.fetchmany(10) to get first 10 rows
    printtable(data, coloumns)
    db_connection.close()
    connection.close_connection()