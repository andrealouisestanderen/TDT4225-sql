from program import Program
from math import sin, cos, sqrt, atan2, radians
from haversine import haversine
"""
    This file stores all the queries.
"""

"""
1. How many users, activities and trackpoints are there in the dataset 
    (after it is inserted into the database).
"""


def NumberOfUsersActivitiesTrackpoints(program):
    querylist = ['SELECT COUNT(*) FROM User', 'SELECT COUNT(*) FROM Activity',
                 'SELECT COUNT(*) FROM TrackPoint']
    tables = ['users', 'activities', 'trackpoints']
    for i, q in enumerate(querylist):
        program.cursor.execute(q)
        result = str(program.cursor.fetchone()[0])
        print("Number of " + tables[i] + ": " + result + ".\n")

    program.db_connection.commit()


"""
2. Find the average number of activities per user.
"""


def AverageNumberOfActivities(program):
    query = ('SELECT AVG(ActivitiesCount) FROM '
             '(SELECT User.id AS UserID, COUNT(*) AS ActivitiesCount FROM '
             'User INNER JOIN Activity ON User.id=Activity.user_id '
             'GROUP BY User.id) AS avgAct')
    program.cursor.execute(query)
    result = str(program.cursor.fetchall()[0])[10:-4]
    print("Average number of activities per user: " + result)
    program.db_connection.commit()


"""
3. Find the top 20 users with the highest number of activities.
"""


def TopNUsersMostActivities(program, n):
    query = ('SELECT User.id AS UserID, COUNT(*) AS ActivitiesCount FROM '
             'User INNER JOIN Activity ON User.id=Activity.user_id '
             'GROUP BY User.id ORDER BY ActivitiesCount DESC LIMIT ' + str(n))
    program.cursor.execute(query)
    result = program.cursor.fetchall()
    for i, res in enumerate(result):
        print("Rank:    " + str(i+1) + "    |     "
              "User:    " + str(res[0]) + "    |     "
              "# of activities:  " + str(res[1]) + ".")
    program.db_connection.commit()


"""
4. Find all users who have taken a taxi.
"""


def UsersTakeTaxi(program):
    query = ('SELECT DISTINCT User.id, '
             'Activity.transportation_mode FROM User '
             'INNER JOIN Activity ON User.id=Activity.user_id '
             'WHERE Activity.transportation_mode="taxi"')
    program.cursor.execute(query)
    result = program.cursor.fetchall()
    print("Users who have taken taxi: \n")
    for res in result:
        print("|    User:       " + str(res[0]) + "     |")
    print("\n\nTotal number of users who have taken taxi: " + str(len(result)))
    program.db_connection.commit()


"""
5. Find all types of transportation modes and count how many activities that are
    tagged with these transportation mode labels. Do not count the rows where the
    mode is null.
"""


def TypesAndAmountofTransportationModes(program):
    query = ('SELECT Activity.transportation_mode, '
             'Count(Activity.transportation_mode) AS TransportationCount '
             'FROM Activity WHERE Activity.transportation_mode!="-" '
             'GROUP BY Activity.transportation_mode '
             'ORDER BY TransportationCount DESC')
    program.cursor.execute(query)
    result = program.cursor.fetchall()
    print("Transportation modes: \n")
    for res in result:
        print("|    Transportation mode: " + str(res[0] + ",").ljust(15) +
              "Number of times used: " + str(res[1]).ljust(15) + "|")
    program.db_connection.commit()


"""
6. a) Find the year with the most activities.
    b) Is this also the year with most recorded hours?
"""


def YearMostActivities(program):
    query = ('SELECT Year, ActivitiesPerYear FROM '
             '(SELECT YEAR(Activity.start_date_time) AS Year, '
             'COUNT(*) AS ActivitiesPerYear FROM Activity '
             'GROUP BY Year '
             'ORDER BY ActivitiesPerYear DESC) AS YearAct')
    program.cursor.execute(query)
    result = program.cursor.fetchall()[0]
    print("Year: " + str(result[0]) +
          " has the most activities, with: " +
          str(result[1]) + " actvivities.")
    program.db_connection.commit()
    return str(result[0])


def YearMostRecordedHours(program):
    query = ('SELECT Year, SUM(Hours) AS HoursOfYear FROM '
             '(SELECT YEAR(Activity.start_date_time) AS Year, '
             'TIMEDIFF(Activity.end_date_time, Activity.start_date_time) AS Hours '
             'FROM Activity) AS YearHours '
             'GROUP BY Year '
             'ORDER BY HoursOfYear DESC')
    program.cursor.execute(query)
    result = program.cursor.fetchall()[0]
    print("Year: " + str(result[0]) +
          " has the most recorded hours, with: " +
          str(result[1]) + " recorded hours.\n\n")
    program.db_connection.commit()
    return str(result[0])


def MostActivitiesAndRecordedHours(program):
    most_activities_year = YearMostActivities(program)
    most_recorded_hours_year = YearMostRecordedHours(program)
    same_year = str(most_activities_year == most_recorded_hours_year)
    if most_activities_year == most_recorded_hours_year:
        print("The year with the most activities, " + most_activities_year +
              " is also the year with most recorded hours.")
    else:
        print("The year with the most activities, " + most_activities_year +
              " is not the same year with most recorded hours, " +
              most_recorded_hours_year + ".")


"""
7. Find the total distance (in km) ​walked​ in 2008, by user with id=112.
"""

def DistanceWalked(program, year, user):
    query = ('SELECT TrackPoint.lat, TrackPoint.lon, TrackPoint.activity_id FROM Activity '
             'INNER JOIN TrackPoint ON Activity.id=TrackPoint.activity_id '
             'WHERE Activity.transportation_mode="walk" '
             'AND Activity.user_id="'+str(user)+'" '
             'AND YEAR(Activity.start_date_time)="'+str(year)+'"')
    program.cursor.execute(query)
    result = program.cursor.fetchall()
    tot_dist = 0

    for i in range(len(result)-1):
        # Check if the trackpoints is in the same activity
        if(result[i][2] == result[i+1][2]):
            # Haversine calculate distance between latitude longitude pairs
            dist = haversine(result[i][0:2], result[i+1][0:2])
            tot_dist += dist

    print("Total distance walked in", year,
          "by user with id", user, ":", tot_dist, "km")
    program.db_connection.commit()


"""
8. Find the top 20 users who have gained the most altitude ​meters​.
    ○ Output should be a table with (id, total meters gained per user).
    ○ Remember that some altitude-values are invalid
    ○ Tip: ∑(tp n .altitude − tp n−1 .altitude), tp n .altitude > tp n−1 .altitude
"""


def Top20UsersMostAltitude(program):
    ids = {}
    for id in program.ids:
        altitude_gained = 0
        query = ('SELECT TrackPoint.altitude '
                'FROM Activity JOIN TrackPoint ON Activity.id=TrackPoint.activity_id '
                'WHERE Activity.user_id = %s AND TrackPoint.altitude > 0')
        program.cursor.execute(query % (id))
        results = program.cursor.fetchall()
        
        for i in range(len(results)-1):
            
            if results[i+1][0]>results[i][0]:
                altitude_gained += (results[i+1][0]-results[i][0])
        if ids:
            ids = {k: v for k, v in sorted(ids.items(), key=lambda item: item[1])}
            if altitude_gained > ids[next(iter(ids))]:
                if len(ids) == 20:
                    del ids[next(iter(ids))] # removes first element in ids
                    ids[id] = altitude_gained
                else:
                    ids[id] = altitude_gained
        else:
            ids[id] = altitude_gained
    
    for id in ids:
        print('User: ' + str(id) + ' with altitude: ' + str(ids[id]) + '\n')



"""
9. Find all users who have invalid activities, and the number of invalid activities per user
    ○ An invalid activity is defined as an activity with consecutive trackpoints where 
        the timestamps deviate with at least 5 minutes.
"""


def UsersAmountOfInvalidActivities(program):
    query = 'SELECT user_id, COUNT(activity_id) ' \
            'FROM (Activity A ' \
            'JOIN (SELECT DISTINCT T1.activity_id ' \
            'FROM TrackPoint T1 JOIN TrackPoint T2 ON T1.activity_id = T2.activity_id ' \
            'WHERE TIMESTAMPDIFF(MINUTE, T1.date_time, T2.date_time) > 5 AND (T1.id + 1) = T2.id) ' \
            'AS inv_act ON A.id = inv_act.activity_id)' \
            'GROUP BY A.user_id;'
    program.cursor.execute(query)
    result = program.cursor.fetchall()
    program.db_connection.commit()
    print(result)


"""
10. Find the users who have tracked an activity in the Forbidden City of Beijing.
    ○ In this question you can consider the Forbidden City to have coordinates
        that correspond to: ​lat ​39.916, lon 116.397.
"""


def UsersActivityWithCoordinates(program):
    query = ('SELECT DISTINCT user_id '
             'FROM Activity A JOIN TrackPoint T '
             'ON A.id = T.activity_id '
             'WHERE T.lon LIKE "116.397%" '
             'AND T.lat LIKE "39.916%" ;')
    program.cursor.execute(query)
    result = program.cursor.fetchall()
    program.db_connection.commit()
    print(result)


"""
11. Find all users who have registered transportation_mode and their most used
    transportation_mode.
    ○ The answer should be on format (user_id,
        most_used_transportation_mode) sorted on user_id.
    ○ Some users may have the same number of activities tagged with e.g. walk
        and car. In this case it is up to you to decide which transportation mode
        to include in your answer (choose one).
    ○ Do not count the rows where the mode is null.
"""


def UsersMostUsedTransportationMode(program):
    query = 'SELECT max_count.user_id, transportation_mode ' \
            'FROM (SELECT user_id, MAX(mode_count) as maxcount FROM (SELECT user_id, COUNT(transportation_mode) ' \
            'as mode_count, transportation_mode ' \
            'FROM Activity A WHERE transportation_mode != "-" ' \
            'GROUP BY transportation_mode, user_id ' \
            'ORDER BY user_id) as tm_mode ' \
            'GROUP BY user_id) as max_count ' \
            'JOIN (SELECT user_id, COUNT(transportation_mode) as mode_count, transportation_mode ' \
            'FROM Activity A WHERE transportation_mode != "-" GROUP BY transportation_mode, user_id ORDER BY user_id) ' \
            'as transportation_count ON max_count.user_id = transportation_count.user_id ' \
            'WHERE max_count.maxcount = transportation_count.mode_count GROUP BY 1'
    program.cursor.execute(query)
    result = program.cursor.fetchall()
    program.db_connection.commit()
    print(result)
