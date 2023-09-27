from DbConnector import DbConnector
from helpers import printtable

# Find the top 15 users with the highest number of activities

def top_15_users_by_higest_num_activities():
    connection = DbConnector()
    db_connection = connection.db_connection
    cursor = connection.cursor

    query = """ 
            SELECT User.id AS user_id, COUNT(Activity.id) AS activity_count
            FROM User LEFT JOIN Activity ON User.id = Activity.id
            GROUP BY User.id
            ORDER BY activity_count DESC
            LIMIT 15;
            """
    
    cursor.execute(query)
    result = cursor.fetchall()

    if result:
        columns = ["User ID", "Activity count"]
        result = [[row["user_id"], row["activity_count"]] for row in result]
        printtable(result, columns)
    else:
        print("No data found")
    
    db_connection.close()
    connection.close_connection()

if __name__ == "__main__":
    top_15_users_by_higest_num_activities()