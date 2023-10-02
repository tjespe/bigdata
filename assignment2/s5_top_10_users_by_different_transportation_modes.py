from DbConnector import DbConnector
from helpers import print_table

# List the top 10 users by their amount of different transportation modes.

def top_10_users_by_different_transportation_modes():
    connection = DbConnector()
    db_connection = connection.db_connection
    cursor = connection.cursor

    query = """
            SELECT User.id, COUNT(DISTINCT Activity.transportation_mode) AS num_different_transports
            FROM User INNER JOIN Activity ON User.id = Activity.user_id
            GROUP BY User.id 
            ORDER BY num_different_transports DESC
            LIMIT 10
            """

    cursor.execute(query)
    result = cursor.fetchall()

    if result:
            columns = ["User", "Number of different transports"]
            print_table(result, columns)
    else:
            print("No data found")
    
    db_connection.close()
    connection.close_connection()

if __name__ == "__main__":
    top_10_users_by_different_transportation_modes()
        

