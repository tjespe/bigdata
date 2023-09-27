from DbConnector import DbConnector
from helpers import print_table

# Find the users that have traveled the longest total distance in one day for each transportation mode

def user_traveled_longest_in_one_day():
    connection = DbConnector()
    db_connection = connection.db_connection
    cursor = connection.cursor

    query = """ 
            SELECT Activity.transportation_mode AS transportation_mode, User.id AS user_id, SUM(Distance) AS total_distance
            FROM User LEFT JOIN Activity ON User.id = Activity.user_id



            GROUP BY transportation_mode, user_id, bruker og dag, 
            ORDER BY total_distance DESC
            
            """
    
    cursor.execute(query)
    result = cursor.fetchall()

    if result:
        columns = ["User ID", "Activity count"]
        print_table(result, columns)
    else:
        print("No data found")
    
    db_connection.close()
    connection.close_connection()

if __name__ == "__main__":
    user_traveled_longest_in_one_day()