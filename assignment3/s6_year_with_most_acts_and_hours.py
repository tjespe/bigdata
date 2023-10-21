from DbConnector import DbConnector


def year_with_most_activities():
    connection = DbConnector()
    client = connection.client
    db = connection.db


    query = db.activities.aggregate([
        {
            "$group": {
               "_id": {
                "$year": "start_time"
               },

                "count": {"$sum":1}
            }
        },
        {
            "$sort": { 
                {"count": -1}
            }
        },
        {
            "$limit": 1
        }

    ])
    year  = query[0]["_id"]
    count = query[0["count"]]

    if query:
        print("The year with the most activities is: ", year, " with ", count, " activities!")
    else:
        print("Something went wrong")

    client.close()

def year_with_most_activity_hours():
    connection = DbConnector()
    client = connection.client
    db = connection.db


    query = db.activities.aggregate([
       {
            "$addFields": {
                 "duration": {
                      "$divide" : [
                      { "$subtract": [ "$end_time", "$start_time" ] },
                      3600000
                 ]
                  }
             }
       },
       {
            "$group":{
                 "_id": 
                      {
                "$year": "start_time"
               },
               "hours": {"$sum": "$duration"}
                 
                }
        },
        {"$sort":{"totalHours":-1}
         },
         {"$limit":1
          }
        


    ])

    year  = query[0]["_id"]
    hours = query[0["hours"]]
    if query:
        print("The year with the most activityhours is: ", year, " with ", hours, " total hours!")
    else:
        print("Something went wrong")

    client.close()


year_with_most_activities()
year_with_most_activity_hours()