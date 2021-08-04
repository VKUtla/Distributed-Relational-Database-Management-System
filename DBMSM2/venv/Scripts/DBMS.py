import Table

class DBMS:

    def __init__(self, DBMSName):
        self.DBMSName = DBMSName
        self.databases = set()
        self.tables = {}


    def getTables(self):
        return tables

    def getDBMSName(self):
        return self.DBMSName

    def addDatabase(self, database):
        for databaseRecord in self.databases:
            if databaseRecord == database:
                return False
        self.databases.add(database)
        return True

    def deleteDatabase(self, database):
        emptyDatabase = True
        for databaseRecord in self.databases:
            if databaseRecord == database:
                for record in self.tables:
                    if record[0] == database:
                        self.tables.remove({record[0], record[1]})
                self.databases.remove(database)
                return True
        print("Database does not exist")
        return False

    def addTable(self, databaseName, table):
        databaseFlag = False
        if len(self.tables) == 0:
            db = {databaseName: table}
            self.tables.update(db)
            return True
        for record in self.tables:
            if record == databaseName:
                databaseFlag = True
                if self.tables.get(record).getTableName == table.getTableName():
                    return False
        if databaseFlag == True:
            db = {databaseName: table}
            self.tables.update(db)
            #self.tables.__add__((databaseName,table))
            return True
        else:
            return False

    def getTable(self, database, tableName):
        for record in self.tables:
            if record == database and self.tables.get(record).getTableName() == tableName:
                return self.tables.get(record)
        return None


    def deleteTable(self, databaseName, table):
        databaseFlag = False
        for record in self.tables:
            if record[0] == databaseName:
                if record[1].getTableName() == table.getTableName():
                    self.tables.remove({record[0], record[1]})
                    return True
        return False

    def updateTable(self, tableName):
        for record in self.tables:
            if record[1].getTableName() == tableName:
                return record[1].update()
        return False