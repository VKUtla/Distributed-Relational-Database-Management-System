import re
#from GDD import GDD
#from DBMS import DBMS
#from Table import Table
class UpdateParser:
    def updateParser(self, query, DBMSObj1, DBMSObj2, GDDObj):
        try:
            regEx = "UPDATE\\s(\\w+)\\sSET\\s(.*)\\sWHERE\\s(.*)?"
            setConstraints = {}
            elements = re.match(regEx, query, re.IGNORECASE)
            individualSets = elements[2]
            if individualSets is not None:
                singleSet = individualSets.split(",")
                for data in singleSet:
                    setElements = data.split("=")
                    columnName = setElements[0].replace(" ","")
                    columnValue = setElements[1].replace(" ","")
                    setConstraints.update({columnName: columnValue})


            tableName = elements[1]
            queryCondition = elements[3]

            if queryCondition is None:
                print("No 'where' condition is specified.")
                return False


            if '=' in queryCondition and '>=' not in queryCondition and '<=' not in queryCondition:
                queryCondition = queryCondition.replace(" ", "")
                parameters = queryCondition.split("=")
                params = {parameters[0]: parameters[1]}
                constraintType = 'equal'
            elif '>' in queryCondition and '=' not in queryCondition:
                queryCondition = queryCondition.replace(" ", "")
                parameters = queryCondition.split(">")
                params = {parameters[0]: parameters[1]}
                constraintType = 'greaterThan'
            elif '<' in queryCondition and '=' not in queryCondition:
                queryCondition = queryCondition.replace(" ", "")
                parameters = queryCondition.split("<")
                params = {parameters[0]: parameters[1]}
                constraintType = "lessThan"
            elif '>=' in queryCondition:
                parameters = queryCondition.replace(" ", "")
                params = queryCondition.split(">=")
                params = {parameters[0]: parameters[1]}
                constraintType = 'greaterEqual'
            elif '<=' in queryCondition:
                queryCondition = queryCondition.replace(" ", "")
                parameters = queryCondition.split("<=")
                params = {parameters[0]: parameters[1]}
                constraintType = 'lesserEqual'

            param = GDDObj.getDatabaseName(tableName)
            if param is None:
                print("Table does not exist")
                return
            DBMSName = param[0].getDBMSName()
            Database = param[1]
            if DBMSObj1.getDBMSName() == DBMSName:
                table = DBMSObj1.getTable(Database, tableName)
                if table is not None:
                    return table.update(tableName, setConstraints, params, constraintType)
            else:
                table = DBMSObj2.getTable(Database, tableName)
                if table is not None:
                    return table.update(tableName, setConstraints, params, constraintType)
           # return UpdateTable(Constants.DBNAME, tableName, setConstraints, params, constraintType)

        except:
            print("Incorrect syntax")
            return None



