from DbConnector import DbConnector
from helpers import print_table

# Find the number of users which have been close to each other in time and space.
# Close is defined as the same space (50 meters) and for the same half minute (30 seconds)

def most_used_transportation_mode_per_user():
    connection = DbConnector()
    db_connection = connection.db_connection
    cursor = connection.cursor

    query = """
            SELECT *
            FROM User INNER JOIN Activity on User.id = Activity.user_id
            WHERE Activity.transportation_mode IS NOT NULL
            GROUP BY User.id

            

            """

    cursor.execute(query)
    result = cursor.fetchall()

    if result:
        columns = ["XX"]
        print_table(result, columns)
    else:
        print("No data found")
    
    db_connection.close()
    connection.close_connection()

if __name__ == "__main__":
    most_used_transportation_mode_per_user()