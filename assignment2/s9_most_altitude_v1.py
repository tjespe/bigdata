from DbConnector import DbConnector
from helpers import print_table


def get_users_with_most_altitude_gain():
    connection = DbConnector()
    cursor = connection.cursor
    
    query = """
        WITH LaggedTrackPointData AS (
            SELECT 
                tp.activity_id,
                tp.altitude,
                LAG(tp.altitude, 1, 0) OVER (PARTITION BY tp.activity_id ORDER BY tp.date_days) AS prev_altitude
            FROM 
                TrackPoint tp
        )

        SELECT
            a.user_id,
            SUM(
                CASE 
                    WHEN l.altitude > l.prev_altitude THEN l.altitude - l.prev_altitude
                    ELSE 0
                END
            ) as alt_gain_sum
        FROM 
            LaggedTrackPointData l
        JOIN 
            Activity a ON a.id = l.activity_id
        GROUP BY
            a.user_id
        ORDER BY alt_gain_sum DESC
        LIMIT 15
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    print("Users with most altitude gain:")
    print_table(rows, ["User ID", "Altitude Gain Sum"])
    return rows

if __name__ == "__main__":
    get_users_with_most_altitude_gain()