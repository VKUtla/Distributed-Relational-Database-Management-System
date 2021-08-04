class GDD:

    def __init__(self):
        self.mappings = []
        self.databaseAndDBMS = []

    def getDBMS(self, databaseName):
        for record in self.databaseAndDBMS:
            if record[1] == databaseName:
                return record[0]
        return None

    def addDatabaseAndDBMS(self, databaseName, DBMSName):
        for record in self.databaseAndDBMS:
            if record[0] == DBMSName and record[1] == databaseName:
                return False
        self.databaseAndDBMS.append((DBMSName, databaseName))

    def addRecord(self, database, DBMSName, tableName):
        for map in self.mappings:
            databaseName = map[0]
            if databaseName == database:
                return None     #"Table already exists
        self.mappings.append((DBMSName, database, tableName))
        return "Table added to "+DBMSName.getDBMSName()

    def removeRecord(self, database, DBMSName, tableName):
        for map in self.mappings:
            databaseName = map[1]
            if databaseName == database and tableName == map[2]:
                self.mappings.remove((DBMSName, database, tableName))
                print("Table removed successfully")
                return True
        return None     #Table not found

    def removeRecordDatabase(self, database, DBMSName):
        for map in self.mappings:
            databaseName = map[1]
            if databaseName == database and DBMSName == map[0]:
                self.mappings.remove((DBMSName, database, map[2]))

        for record in self.databaseAndDBMS:
            if record[1] == database and DBMSName == record[0]:
                self.databaseAndDBMS.remove((record[0], record[1]))
        return True

    def getRecords(self):
        return self.mappings

    def getDatabaseName(self, tableName):
        for map in self.mappings:
            #print(map[0].getDBMSName, database)
            tableNameRecord = map[2]
            if tableNameRecord == tableName:
                return (map[0], map[1])
        return None     #Table not found.