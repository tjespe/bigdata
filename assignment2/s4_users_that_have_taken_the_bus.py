from DbConnector import DbConnector
from helpers import print_table

# Find all users who have taken a bus.

def users_that_have_taken_the_bus():
    connection = DbConnector()
    db_connection = connection.db_connection
    cursor = connection.cursor

    query = """ 
            SELECT DISTINCT User.id AS user_id
            FROM User LEFT JOIN Activity ON User.id = Activity.user_id
            WHERE Activity.transportation_mode = 'bus'
            GROUP BY User.id
            """
    
    cursor.execute(query)
    result = cursor.fetchall()

    if result:
        columns = ["User ID"]
        print_table(result, columns)
    else:
        print("No data found")
    
    db_connection.close()
    connection.close_connection()

if __name__ == "__main__":
    users_that_have_taken_the_bus()