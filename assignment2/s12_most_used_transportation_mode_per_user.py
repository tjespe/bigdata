from DbConnector import DbConnector
from helpers import print_table

# Find all users who have registered transportation_mode and their most used transportation_mode.

def most_used_transportation_mode_per_user():
    connection = DbConnector()
    db_connection = connection.db_connection
    cursor = connection.cursor

    query = """
            SELECT user_id, 
            (
                SELECT transportation_mode
                FROM Activity AS sub_a
                WHERE transportation_mode IS NOT NULL AND a.user_id = sub_a.user_id
                GROUP BY transportation_mode
                ORDER BY COUNT(*) DESC
                LIMIT 1
            ) AS most_used_transportation_mode
            FROM Activity AS a
            WHERE transportation_mode IS NOT NULL
            GROUP BY user_id
            ORDER BY user_id
            """

    cursor.execute(query)
    result = cursor.fetchall()

    if result:
        columns = ["User ID", "Most Used Transportation Mode"]
        print_table(result, columns)
    else:
        print("No data found")
    
    db_connection.close()
    connection.close_connection()

if __name__ == "__main__":
    most_used_transportation_mode_per_user()