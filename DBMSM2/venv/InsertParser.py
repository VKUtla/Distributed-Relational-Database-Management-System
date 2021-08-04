import re
class InsertParser:

    def insertParser(self, query, DBMSLocal, DBMSRemote, GDD):
        insertRe= "INSERT INTO (\S+) \((.*)\) VALUES \((.*)\)"
        m = re.match(insertRe, query, re.IGNORECASE)
        cols = m[2].split(",")
        values = m[3].split(",")
        tableName = m[1]

        if len(cols) == len(values):
            param = GDD.getDatabaseName(tableName)
            if param is None:
                print("Table does not exist")
                return False
            DBMSName = param[0].getDBMSName()
            Database = param[1]
            if DBMSLocal.getDBMSName() == DBMSName:
                table = DBMSLocal.getTable(Database, tableName)
                if table is not None:
                    return table.insertData(m[3])
            else:
                table = DBMSRemote.getTable(Database, tableName)
                if table is not None:
                    return table.insertData(m[3])
        else:
            return False

        return False