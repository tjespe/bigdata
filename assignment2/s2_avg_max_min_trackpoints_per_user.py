from DbConnector import DbConnector
from helpers import print_table

# Find the average, maximum and minimum number of trackpoints per user.

def avg_max_min_trackpoints_per_user():
    connection = DbConnector()
    db_connection = connection.db_connection
    cursor = connection.cursor

    query = """
        SELECT
            AVG(TrackPointCount) AS Average,
            MAX(TrackPointCount) AS Maximum,
            MIN(TrackPointCount) as Minimum
        FROM (
            SELECT User.id, SUM(TrackPoints) AS TrackPointCount
            FROM User INNER JOIN (
                SELECT Activity.user_id, Activity.id, COUNT(TrackPoint.id) AS TrackPoints
                FROM Activity INNER JOIN TrackPoint ON Activity.id = TrackPoint.activity_id
                GROUP BY Activity.user_id, Activity.id
            ) AS UserActTrackSub ON User.id = UserActTrackSub.user_id
            GROUP BY User.id
        ) AS JoinedTable
    """

    cursor.execute(query)
    result = cursor.fetchall()

    if result:
        columns = ["Average", "Maximum", "Minimum "]
        print_table(result, columns)
    else:
        print("No data found")
    
    db_connection.close()
    connection.close_connection()

if __name__ == "__main__":
    avg_max_min_trackpoints_per_user()