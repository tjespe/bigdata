from DbConnector import DbConnector
import pandas as pd
import numpy as np
import pyntcloud
import matplotlib as plt

def vizualize_data():
    connection = DbConnector()
    db_connection = connection.db_connection
    cursor = connection.cursor

    query = """ 
            WITH Subtable as (
                SELECT
                    User.id as user,
                    Activity.id as actid
                FROM 
                User INNER JOIN Activity ON User.id = Activity.user_id
                WHERE User.id = 14
            )

            SELECT 
                TrackPoint.lat as lat, 
                TrackPoint.lon as lon, 
                TrackPoint.altitude as alt 
            FROM 
            Subtable INNER JOIN TrackPoint on Subtable.actid = TrackPoint.activity_id
            """
    
    cursor.execute(query)
    df = pd.DataFrame(
            cursor.fetchall(),
            columns=["x", "y", "z"]
        ) 
    
    fig = plt.figure()
    ax = fig.add_subplot(111, projection='3d')

    x = df['longitude']
    y = df['latitude']
    z = df['altitude']

    ax.scatter(x, y, z, c='b', marker='o', label='Altitude')

    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    ax.set_zlabel('Altitude')

    plt.show()

    db_connection.close()
    connection.close_connection()

    # P = np.c_[df['lon'], df['lat'], np.zeros(len(df))]

if __name__ == "__main__":
    vizualize_data()