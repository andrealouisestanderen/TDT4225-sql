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
        self.labeled = set(self.file_reader("dataset/labeled_ids.txt", False))

    def create_table(self, table_name, query):
        # This adds table_name to the %s variable and executes the query
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

    def file_reader(self, filepath, read_trajectory):
        f = open(filepath, "r")
        file = []
        i = -6
        for line in f:
            file.append(line.strip())
            i+=1
            if i > 2500: # Checking if file is too long
                f.close()
                return None, None
        if read_trajectory: #Getting the values if it is a trajectory
            first_point = file[6].strip()
            last_point = file[-1]
            start_time = first_point[-19:].replace(',', ' ')
            end_time = last_point[-19:].replace(',', ' ')
            return start_time, end_time
        f.close()
        return file

    def make_user(self):
        for id in self.subfolders:
            if id in self.labeled:
                query = "INSERT INTO User (id, has_labels) VALUES('%s', 1)"
            else:
                query = "INSERT INTO User (id, has_labels) VALUES ('%s', 0);"
            self.cursor.execute(query % (id))
        self.db_connection.commit()
        
    '''def short_file(self, filepath):
        number_of_lines = sum(1 for line in open(filepath))
        return (number_of_lines - 6) <= 2500'''

    '''def read_trajectory(self, filepath):
        trackpoints = self.file_reader(filepath)
        first_point = trackpoints[6].strip()
        last_point = trackpoints[-1]
        start_time = first_point[-19:].replace(',', ' ')
        end_time = last_point[-19:].replace(',', ' ')

        return start_time, end_time'''


    def make_activity(self):
        # Iterates over all the users
        for id in self.ids:
            # Checking if they are labeled
            if id in self.labeled:
                print(id, "is labeled.")
                # Going through the files (bottom up, so the .plt-files)
                for (root, dirs, files) in os.walk('dataset/Data/'+ id, topdown=False):
                   i = 1
                   for file in files:
                        print("FILE NR;", i)
                        i+=1
                        # Checking if .plt-file has under 2500 lines and that it is not a ignore-file
                        print("TRUE OR FALSE:", (not file.startswith('.')) and (self.file_reader(os.path.join(root, file), False)[0]!=None))
                        if (not file.startswith('.')) and (self.file_reader(os.path.join(root, file), False)[0]!=None):
                            #må finne måte å finne riktig linje i labels på, hvis plt-fil ikke er for lang
                            data = self.file_reader('dataset/Data/'+id+'/'+'labels.txt', False)
                            for line in data[1:]:
                                print("...")
                                transportation_mode = line[40:].strip()
                                start_date_time = line[0:20].strip().replace("/", "-")
                                end_date_time = line[20:40].strip().replace("/", "-")
                                query = "INSERT INTO Activity (user_id, transportation_mode, start_date_time, end_date_time) VALUES ('%s', '%s', '%s', '%s')"
                                self.cursor.execute(query % (id, transportation_mode, start_date_time, end_date_time))
                                self.db_connection.commit()
                            break
                        else:
                            continue
                #får dobbelt opp med labeled. Fikk 868 med id 010
            else:
                print(id, "is not labeled.")
                # Going through the files for non-labeled users
                for (root, dirs, files) in os.walk('dataset/Data/'+ id, topdown=False):
                    for file in files:
                        #Ignoring .-files
                        if not file.startswith('.'):
                            # Saving data
                            start_date_time, end_date_time = self.file_reader(os.path.join(root, file), True)
                            #Start_date_time = None if the file is too long
                            if start_date_time:
                                transportation_mode = "-"
                                query = "INSERT INTO Activity (user_id, transportation_mode, start_date_time, end_date_time) VALUES ('%s', '%s', '%s', '%s')"
                                self.cursor.execute(query % (id, transportation_mode, start_date_time, end_date_time))
                                self.db_connection.commit()
                        else:
                            continue
        #self.db_connection.commit()





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

        program.drop_table(table_name="TrackPoint")
        program.drop_table(table_name="Activity")
        program.drop_table(table_name="User")

        program.create_table(table_name="User",query=query_user)
        print("USER")
        program.create_table(table_name="Activity",query=query_activity)
        print("ACTIVITY")
        program.create_table(table_name="TrackPoint", query=query_trackpoint)
        print("TP")   

        program.make_user()
        print("MAKE USER")
        program.make_activity()
        print("MAKE ACTIVITY")

        _ = program.fetch_data(table_name="Activity")

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()



    


if __name__ == '__main__':
    main()
