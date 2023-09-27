from string import printable
from assignment2.DbConnector import DbConnector


def get_users_with_most_altitude_gain():
    connection = DbConnector()
    db_connection = connection.db_connection
    cursor = connection.cursor
    
    query = """
        SELECT user_id, SUM(altitude_gain)
        FROM Activity
        GROUP BY user_id
        ORDER BY SUM(altitude_gain)
        DESC
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    print("Users with most altitude gain:")
    printable(rows)
    return rows

