# Queries to insert: 
"""
1. How many users, activities and trackpoints are there in the dataset 
    (after it is inserted into the database).
"""
def NumberOfUsers():
    query=''

def NumberOfActivities():
    query=''

def NumberOfTrackpoints():
    query=''

"""
2. Find the average number of activities per user.
"""
def AverageNumberOfActivities():
    query=''

"""
3. Find the top 20 users with the highest number of activities.
"""
def TopNUsersMostActivities(n):
    query=''


"""
4. Find all users who have taken a taxi.
"""
def UsersTakeTaxi():
    query=''

"""
5. Find all types of transportation modes and count how many activities that are
    tagged with these transportation mode labels. Do not count the rows where the
    mode is null.
"""
def TypesAndAmountofTransportationModes():
    query=''

"""
6. a) Find the year with the most activities.
    b) Is this also the year with most recorded hours?
"""
def YearMostActivities():
    query=''

def YearMostRecordedHours():
    query=''

def MostActivitiesAndRecordedHours():
    most_activities_year = YearMostActivities()
    most_recorded_hours_year = YearMostRecordedHours()
    return most_activities_year == most_recorded_hours_year
"""
7. Find the total distance (in km) ​walked​ in 2008, by user with id=112.
"""
def DistanceWalked(year, user):
    query=''

"""
8. Find the top 20 users who have gained the most altitude ​meters​.
    ○ Output should be a table with (id, total meters gained per user).
    ○ Remember that some altitude-values are invalid
    ○ Tip: ∑(tp n .altitude − tp n−1 .altitude), tp n .altitude > tp n−1 .altitude
"""
def TopNUsersMostAltitude(n):
    query=''

"""
9. Find all users who have invalid activities, and the number of invalid activities per user
    ○ An invalid activity is defined as an activity with consecutive trackpoints where 
        the timestamps deviate with at least 5 minutes.
"""
def UsersAmountOfInvalidActivities():
    query=''

"""
10. Find the users who have tracked an activity in the Forbidden City of Beijing.
    ○ In this question you can consider the Forbidden City to have coordinates
        that correspond to: ​lat ​39.916, lon 116.397.
"""
def UsersActivityWithCoordinates(lat, lon):
    query=''

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
    quer=''

