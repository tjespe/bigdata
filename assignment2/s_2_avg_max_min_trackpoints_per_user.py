from DbConnector import DbConnector

#Find the average, maximum and minimum number of trackpoints per user.

def s2():
        connection = DbConnector()
        db_connection = connection.db_connection
        cursor = connection.cursor

        query = """
                "SELECT AVG(COUNT(TrackPoint.id)) AS Average, MAX(COUNT(TrackPoint.id)) AS Maximum, MIN(COUNT(TrackPoint.id)) as Minimum
                FROM User INNER JOIN ACTIVITY ON User.id = Activity.user_id INNER JOIN TrackPoint ON Activity.id = TrackPoint.activity_id
                 GROUP BY User.id"
                """