from DbConnector import DbConnector
from helpers import printtable

#List the top 10 users by their amount of different transportation modes.

def s5_top10_users_by_different_transportation_modes():
        connection = DbConnector()
        db_connection = connection.db_connection
        cursor = connection.cursor

        query = """
                SELECT User.id AS User, COUNT(DISTINCT transportation_mode) num_different_transports
                FROM User INNER JOIN Activity ON User.id = Activity.user_id
                GROUP BY User.id 
                ORDER BY num_different_transports 
                LIMIT 10
                """
        cursor.execute(query)
        coloumns = ["User", "Number of different transports"]
        data = cursor.fetchall()
        # can also use cursor.fetchmany(10) to get first 10 rows
        printtable(data, coloumns)
        db_connection.close()
        connection.close_connection()

s5_top10_users_by_different_transportation_modes()