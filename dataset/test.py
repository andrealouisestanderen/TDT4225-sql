

    def file_reader_trackpoint(self, filepath, activity_id):
        data = []
        f = open(filepath, "r")
        for line in f: 
            trackpoint = line.split(",")
            date_time = trackpoint[5].replace("-", "/") + " " + trackpoint[6]
            date_time.strip()
            print(date_time)
            trackpoint[5] = date_time
            trackpoint.pop(2)
            trackpoint.insert(0, activity_id)
            tuppel = tuple(trackpoint[0:5])
            data.append(tuppel)
        return data



    def make_trackpoint(self, activity_id):
        print("MAKE TP")
        #query mangler activity_id
        query = "INSERT INTO TrackPoint (activity_id lat, lon, altitude, date_days, date_time) VALUES (%s, %s, %s, %s, %s, '%s')"
        for id in self.ids:
            print("MAKE TP 1")
            for (root, dirs, files) in os.walk('dataset/Data/'+ id, topdown=False):
                print("MAKE TP 2")
                for file in files:
                    print("MAKE TP 3 ")
                    print("FILE:", file, )

                    print('Longfiles set ' + str(self.long_files.get(id)))
                    if (id in self.long_files and file not in self.long_files.get(id)):
                        print("4")
                        trackpoints = self.file_reader_trackpoint(os.path.join(root, file), activity_id)
                        print("5")
                        self.cursor.executemany(query, trackpoints)