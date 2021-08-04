import re
from Table import Table
from GDD import GDD
from DBMS import DBMS
class CreateParser:

    def createParser(self, query, GDD, databaseName, DBMSLocal, DBMSRemote):
        createRe = "CREATE\s+TABLE\s+(\S+)\s*\((.*)\)"
        elements = re.match(createRe, query, re.IGNORECASE)
        individualSets = elements[2]
        columns = []
        columnTypes = []
        pk = []
        if individualSets is not None:
            columnNames = elements[2].split(",")
            for individualColumn in columnNames:
                columnParameters = individualColumn.split(" ")
                if len(columnParameters) == 3 and len(pk) == 0:
                    pk = columnParameters[0]
                    columns.append(columnParameters[0])
                    columnTypes.append(columnParameters[1])
                elif len(columnParameters) == 2:
                    columns.append(columnParameters[0])
                    columnTypes.append(columnParameters[1])
                else:
                    return False

        table = Table(elements[1], columns, columnTypes, pk)

        DBMSName = GDD.getDBMS(databaseName)
        if DBMSName is not None:
            if DBMSName.getDBMSName() == "local":
                DBMSLocal.addTable(databaseName, table)
                GDD.addRecord(databaseName, DBMSName, table.getTableName())
            elif DBMSName.getDBMSName() == "remote":
                DBMSRemote.addTable(databaseName, table)
                GDD.addRecord(databaseName, DBMSName, table.getTableName())
        else:
            return False
        return True




