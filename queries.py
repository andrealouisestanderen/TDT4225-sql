from program import Program
# Queries to insert:

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
    # Should be derrived in a different manner:
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
    print("Top " + str(n) + " users with highest number of activities: " + str(result))
    for i, res in enumerate(result):
        print("Rank:    " + str(i+1) + "    |     "
            "User:    " + str(res[0]) + "    |     "
            "# of activities:  " + str(res[1]) + ".")
    program.db_connection.commit()


"""
4. Find all users who have taken a taxi.
"""


def UsersTakeTaxi():
    query = ''


"""
5. Find all types of transportation modes and count how many activities that are
    tagged with these transportation mode labels. Do not count the rows where the
    mode is null.
"""


def TypesAndAmountofTransportationModes():
    query = ''


"""
6. a) Find the year with the most activities.
    b) Is this also the year with most recorded hours?
"""


def YearMostActivities():
    query = ''


def YearMostRecordedHours():
    query = ''


def MostActivitiesAndRecordedHours():
    most_activities_year = YearMostActivities()
    most_recorded_hours_year = YearMostRecordedHours()
    return most_activities_year == most_recorded_hours_year


"""
7. Find the total distance (in km) ​walked​ in 2008, by user with id=112.
"""


def DistanceWalked(year, user):
    query = ''


"""
8. Find the top 20 users who have gained the most altitude ​meters​.
    ○ Output should be a table with (id, total meters gained per user).
    ○ Remember that some altitude-values are invalid
    ○ Tip: ∑(tp n .altitude − tp n−1 .altitude), tp n .altitude > tp n−1 .altitude
"""


def TopNUsersMostAltitude(n):
    query = ''


"""
9. Find all users who have invalid activities, and the number of invalid activities per user
    ○ An invalid activity is defined as an activity with consecutive trackpoints where 
        the timestamps deviate with at least 5 minutes.
"""


def UsersAmountOfInvalidActivities():
    query = ''


"""
10. Find the users who have tracked an activity in the Forbidden City of Beijing.
    ○ In this question you can consider the Forbidden City to have coordinates
        that correspond to: ​lat ​39.916, lon 116.397.
"""


def UsersActivityWithCoordinates(lat, lon):
    query = ''


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


def UsersMostUsedTransportationMode():
    query = ''
