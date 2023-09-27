from DbConnector import DbConnector
from helpers import print_table


def get_users_with_most_altitude_gain():
    connection = DbConnector()
    cursor = connection.cursor
    
    query = """
        SELECT
            a.user_id,
            SUM(
                CASE 
                    WHEN t.altitude_diff > 0 THEN altitude_diff
                    ELSE 0
                END
            ) as alt_gain_sum
        FROM 
            TrackPoint t
        JOIN 
            Activity a ON a.id = t.activity_id
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