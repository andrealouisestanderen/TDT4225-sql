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
        # Set of all users that are labeled
        self.labeled = set(self.file_reader("dataset/labeled_ids.txt"))

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
        query = "DROP TABLE %s"
        self.cursor.execute(query % table_name)

    def file_reader(self, filepath):
        f = open(filepath, "r")
        file = f.read().splitlines()
        return file

    def make_user(self):
        for id in self.subfolders:
            if id in self.labeled:
                query = "INSERT INTO User (id, has_labels) VALUES('%s', 1)"
            else:
                query = "INSERT INTO User (id, has_labels) VALUES ('%s', 0);"
            self.cursor.execute(query % (id))
        self.db_connection.commit()
    
    def make_activity(self):
        '''for i,(root, dir, files) in enumerate(os.walk('dataset/Data', topdown=True)):
            print("1")
            print("counter:", i)'''
        for id in self.subfolders:
            if id in self.labeled:
                for (files) in os.walk('dataset/Data/'+ id, topdown=True):
                    transportation_modes = self.file_reader('dataset/Data/'+id+'/'+'labels.txt')
                    for transportation_mode in transportation_modes[1:]:
                        mode = transportation_mode[40:].strip()
                        start_date_time = transportation_mode[0:20].strip().replace("/", "-")
                        end_date_time = transportation_mode[20:40].strip().replace("/", "-")
                        query = "INSERT INTO Activity (user_id, transportation_mode, start_date_time, end_date_time) VALUES ('%s', '%s', '%s', '%s')"
                        self.cursor.execute(query % (id, mode, start_date_time, end_date_time))
                        
        self.db_connection.commit()
        print("Done")
        
        # activity in dir:
                


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
