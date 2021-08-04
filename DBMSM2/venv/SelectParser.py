import re
class SelectParser:

    def selectParser(self, query, DBMSObj1, DBMSObj2, GDDObj):
        whereCondition = ''
        whereConditionColumn = ''
        whereConditionValue = ''
        # find own re expresion
        selectRe = "SELECT\s((?:\*)|(?:(?:\w+)(?:,\s?\w+)*))\sFROM\s(\w+)(?:\sWHERE\s(.*))?"
        m = re.match(selectRe, query, re.IGNORECASE)
        column = m[1]

        if "*" in column:
            column = "ALL"

        if m[3]==None:
            whereCondition = ''
            whereConditionColumn=''
            whereConditionValue=''

        elif "!" in m[3]:
            whereCondition = 'NE'
            whereConditionColumn = m[3].split('!=')[0]
            whereConditionValue = m[3].split('!=')[1].strip('"')

        elif "=" in m[3] and "<=" not in m[3] and ">=" not in m[3]:
            whereCondition = 'EQ'
            whereConditionColumn = m[3].split('=')[0]
            whereConditionValue = m[3].split('=')[1].strip('"')

        elif "<" in m[3] and "=" not in m[3]:
            whereCondition = 'L'
            whereConditionColumn = m[3].split('<')[0]
            whereConditionValue = m[3].split('<')[1].strip('"')

        elif ">" in m[3] and "=" not in m[3]:
            whereCondition = 'G'
            whereConditionColumn = m[3].split('>')[0]
            whereConditionValue = m[3].split('>')[1].strip('"')

        elif "<" in m[3] and "=" in m[3]:
            whereCondition = 'LE'
            whereConditionColumn = m[3].split('<=')[0]
            whereConditionValue = m[3].split('<=')[1].strip('"')

        elif ">" in m[3] and "=" in m[3]:
            whereCondition = 'GE'
            whereConditionColumn = m[3].split('>=')[0]
            whereConditionValue = m[3].split('>=')[1].strip('"')

        else:
            print("wrong SQL Query")

        tableName = m[2]
        param = GDDObj.getDatabaseName(tableName)

        if param is None:
            print("Table does not exist")
            return False

        DBMSName = param[0].getDBMSName()
        Database = param[1]
        if DBMSObj1.getDBMSName() == DBMSName:
            table = DBMSObj1.getTable(Database, tableName)
            if table is not None:
                return table.selectExecution(tableName,column, whereCondition, whereConditionColumn, whereConditionValue)
        else:
            table = DBMSObj2.getTable(Database, tableName)
            if table is not None:
                return table.selectExecution(tableName,column, whereCondition, whereConditionColumn, whereConditionValue)
