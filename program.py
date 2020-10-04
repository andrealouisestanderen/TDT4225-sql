from DbConnector import DbConnector
from tabulate import tabulate
import os
import datetime

class Program:

    

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

        # Global variables
        # Read id for each person
        self.subfolders = [ f.name for f in os.scandir("dataset/Data") if f.is_dir() ]
        self.subfolders.sort()
        self.ids = tuple(self.subfolders)
        # Set of all users that are labeled
        self.labeled = set(self.file_reader("dataset/labeled_ids.txt", False, None))
        self.long_files = {}

    def create_table(self, table_name, query):
        # This adds table_name to the %s variable and executes the query
        print("Creating table %s..." % table_name)
        self.cursor.execute(query % table_name)
        self.db_connection.commit()

    def fetch_data(self, table_name):
        query = "SELECT * FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        #print("Data from table %s, raw format:" % table_name)
        #print(rows)
        # Using tabulate to show the table in a nice way
        print("Data from table %s, tabulated:" % table_name)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def drop_table(self, table_name):
        print("Dropping table %s..." % table_name)
        query = "DROP TABLE IF EXISTS %s"
        self.cursor.execute(query % table_name)

    def file_reader(self, filepath, read_trajectory, user_id):
        f = open(filepath, "r")
        file = []
        long_filenames = set()
        if read_trajectory: #Getting the values if it is a trajectory
            i = -6
            for line in f:
                file.append(line.strip())
                i+=1
                if i > 2500: # Checking if file is too long
                    if user_id in self.long_files:
                        self.long_files[user_id].add(filepath[-18:])
                    else:
                        long_filenames.add(filepath[-18:])
                        self.long_files[user_id] = long_filenames
                    f.close()
                    return None, None
            f.close()
            first_point = file[6].strip()
            last_point = file[-1]
            start_time = first_point[-19:].replace(',', ' ')
            end_time = last_point[-19:].replace(',', ' ')
            return start_time, end_time
        else:
            for line in f:
                file.append(line.strip())
            f.close()
            return file


    def read_labels(self, filepath):
        file = self.file_reader(filepath, False, None)
        dict = {} # start_time : transportation_mode
        for line in file:
            transportation_mode = line[40:].strip()
            start_date_time = line[0:20].strip().replace("/", "-")
            dict[start_date_time] = transportation_mode
        return dict

    def make_user(self):
        for id in self.subfolders:
            if id in self.labeled:
                query = "INSERT INTO User (id, has_labels) VALUES('%s', 1)"
            else:
                query = "INSERT INTO User (id, has_labels) VALUES ('%s', 0);"
            self.cursor.execute(query % (id))
        self.db_connection.commit()

    def make_activity(self):
        # Iterates over all the users
        activity_id = 1
        for user_id in self.ids:
            # Checking if they are labeled
            if user_id not in self.labeled:
                print(user_id, "is not labeled.")
                # Going through the files for non-labeled users
                for (root, dirs, files) in os.walk('dataset/Data/'+ user_id, topdown=False):
                    for file in files:
                        #Ignoring .-files
                        if not file.startswith('.'):
                            # Saving data
                            start_date_time, end_date_time = self.file_reader(os.path.join(root, file), True, user_id)
                            #Start_date_time = None if the file is too long
                            if start_date_time:
                                transportation_mode = "-"
                                query = "INSERT INTO Activity (user_id, transportation_mode, start_date_time, end_date_time) VALUES ('%s', '%s', '%s', '%s')"
                                self.cursor.execute(query % (user_id, transportation_mode, start_date_time, end_date_time))
                                self.db_connection.commit()
                                self.make_trackpoint(activity_id, user_id)
                                print("ACTIVITY IS INSERTED:", activity_id)
                                activity_id+=1
                        else:
                            continue
                    
            else:
                print(user_id, "is labeled.... :'(")
                for (root, dirs, files) in os.walk('dataset/Data/'+user_id, topdown=False):
                    for file in files:
                        if not (file.startswith('.') or file =="labels.txt"):
                            start_date_time, end_date_time = self.file_reader(os.path.join(root, file), True, user_id)
                            if start_date_time:
                                labels = self.read_labels('dataset/Data/'+user_id+'/labels.txt')
                                if start_date_time in labels:
                                    transportation_mode = labels.get(start_date_time)
                                else:
                                    transportation_mode = "-"
                                query = "INSERT INTO Activity (user_id, transportation_mode, start_date_time, end_date_time) VALUES ('%s', '%s', '%s', '%s')"
                                self.cursor.execute(query % (user_id, transportation_mode, start_date_time, end_date_time))
                                self.db_connection.commit()
                                self.make_trackpoint(activity_id, user_id)
                                print("ACTIVITY IS INSERTED:", activity_id)
                                activity_id+=1
                        else:
                            continue


    def file_reader_trackpoint(self, filepath, activity_id):
        data = []
        f = open(filepath, "r")
        i=0
        for line in f:
            i+=1
            if i>6:
                trackpoint = line.split(",")

                trackpoint[0]=float(trackpoint[0])
                trackpoint[1]=float(trackpoint[1])
                trackpoint[3]=float(trackpoint[3])
                trackpoint[4]=float(trackpoint[4])

                date_time = trackpoint[5].strip() + " " + trackpoint[6].strip()
                trackpoint[5] = date_time
                trackpoint.pop(2)
                trackpoint.insert(0, activity_id)
                trackpoint=trackpoint[0:6]
                tuppel = tuple(trackpoint)
                data.append(tuppel)

        return data



    def make_trackpoint(self, activity_id, user_id):
        #query mangler activity_id
        query = "INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time) VALUES (%s, %s, %s, %s, %s, %s)"
        for (root, dirs, files) in os.walk('dataset/Data/'+ user_id, topdown=False):
            for file in files:
                if (not (file.startswith('.'))) and (user_id in self.long_files and file not in self.long_files.get(user_id)):
                    trackpoints = self.file_reader_trackpoint(os.path.join(root, file), activity_id)
                    self.cursor.executemany(query, trackpoints)
                    self.db_connection.commit()

    def make_trackpoint_test(self, activity_id, user_id):
        trackpoints = []
        i = 0
        print("Start")
        for (root, dirs, files) in os.walk('dataset/Data/'+ user_id, topdown=False):
            print("Root: ", root)
            print("Dirs: ", dirs)
            print("Files: ", files)
            for file in files:
                if not (file.startswith('.')):
                    trackpoints.append(self.file_reader_trackpoint(os.path.join(root, file), activity_id))
                    print("Added trackpoint" + str(i))
                    i+=1
        return trackpoints

    def insert_data(self):
        for id in self.subfolders:
            if id in self.labeled:
                query = "INSERT INTO User (id, has_labels) VALUES('%s', 1)"
            else:
                query = "INSERT INTO User (id, has_labels) VALUES ('%s', 0);"
            self.cursor.execute(query % id)
            print("Inserted user %s" % id)
            activity_id = 1
            # Going through the files for non-labeled users
            if id not in self.labeled:
                labeled = False
            else:
                labeled = True
            for (root, dirs, files) in os.walk('dataset/Data/'+ id, topdown=False):
                for file in files:
                    # Ignoring .-files, labels.txt and files with more than 2500 trackpoints
                    if not file.startswith('.') and file != 'labels.txt' and self.count_lines(os.path.join(root, file)) < 2506:
                        # Saving data
                        start_date_time, end_date_time = self.file_reader(os.path.join(root, file), True, id)
                        # Start_date_time = None if the file is too long
                        if start_date_time:
                            if labeled:
                                labels = self.read_labels('dataset/Data/' + id + '/labels.txt')
                                if start_date_time in labels:
                                    transportation_mode = labels.get(start_date_time)
                                else:
                                    transportation_mode = '-'
                            else:
                                transportation_mode = '-'
                            query = "INSERT INTO Activity (user_id, transportation_mode, start_date_time, end_date_time) VALUES ('%s', '%s', '%s', '%s')"
                            self.cursor.execute(
                                query % (id, transportation_mode, start_date_time, end_date_time))
                            self.db_connection.commit()
                            query = "INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time) VALUES (%s, %s, %s, %s, %s, %s)"
                            trackpoints = self.file_reader_trackpoint(os.path.join(root, file), activity_id)
                            self.cursor.executemany(query, trackpoints)
                            self.db_connection.commit()
                            print("ACTIVITY IS INSERTED:", activity_id)
                            activity_id += 1

                    else:
                        continue

        self.db_connection.commit()


    @staticmethod
    def count_lines(file):
        with open(file) as f:
            for i, l in enumerate(f):
                pass
        return i + 1

def main():
    query_user = """CREATE TABLE IF NOT EXISTS %s (
                    id VARCHAR(30) NOT NULL PRIMARY KEY,
                    has_labels BOOLEAN);
                """

    query_activity = """CREATE TABLE IF NOT EXISTS %s (
                        id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                        user_id VARCHAR(30) REFERENCES User(id),
                        transportation_mode VARCHAR(30),
                        start_date_time DATETIME,
                        end_date_time DATETIME,
                        CONSTRAINT activity_fk FOREIGN KEY (user_id) REFERENCES User(id) ON UPDATE CASCADE ON DELETE CASCADE
                        );
                     """

    query_trackpoint = """CREATE TABLE IF NOT EXISTS %s (
                          id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                          activity_id INT REFERENCES Activity(id),
                          lat DOUBLE, 
                          lon DOUBLE,
                          altitude INT,
                          date_days DOUBLE,
                          date_time DATETIME,
                          CONSTRAINT trackpoint_fk FOREIGN KEY (activity_id) REFERENCES Activity(id) ON UPDATE CASCADE ON DELETE CASCADE
                          );
                       """

    program = None

    try:
        program = Program()

        #program.drop_table(table_name="TrackPoint")
        #program.drop_table(table_name="Activity")
        #program.drop_table(table_name="User")

        #program.create_table(table_name="User",query=query_user)
        #program.create_table(table_name="Activity",query=query_activity)
        #program.create_table(table_name="TrackPoint", query=query_trackpoint)

        #program.make_user()
        #program.make_activity()
        #program.insert_data()

        _ = program.fetch_data(table_name="TrackPoint")

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()



if __name__ == '__main__':
    main()
