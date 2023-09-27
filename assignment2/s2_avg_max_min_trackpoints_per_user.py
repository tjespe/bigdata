from DbConnector import DbConnector
from helpers import printtable

#Find the average, maximum and minimum number of trackpoints per user.

def s2_avg_max_min_trackpoints_per_user():
        connection = DbConnector()
        db_connection = connection.db_connection
        cursor = connection.cursor

        query = """
                SELECT AVG(COUNT(TrackPointCount)) AS Average, MAX(COUNT(TrackPointCount)) AS Maximum, MIN(COUNT(TrackPointCount)) as Minimum
                FROM (
                        SELECT COUNT(TrackPoint.id) AS TrackPointCount
                        FROM User INNER JOIN Activity ON User.id = Activity.user_id INNER JOIN TrackPoint ON Activity.id = TrackPoint.activity_id
                        GROUP BY User.id, Activity.id
                ) AS JoinedTable
                """
        cursor.execute(query)
        coloumns = ["Average", "Maximum", "Minimum "]
        data = cursor.fetchall()
        printtable(data, coloumns)
        db_connection.close()
        connection.close_connection()

s2_avg_max_min_trackpoints_per_user()
