import re
class DeleteParser:
    def deleteParser(self, query, DBMSObj1, DBMSObj2, GDDObj):

        whereCondition = ''
        whereConditionColumn = ''
        whereConditionValue = ''
        selectRe = "DELETE\sFROM\s(\w+)(?:\sWHERE\s(.*))?"
        m = re.match(selectRe, query, re.IGNORECASE)
        column = m[2]

        if "!" in m[2]:
            whereCondition = 'NE'
            whereConditionColumn = m[2].split('!=')[0]
            whereConditionValue = m[2].split('!=')[1].strip('"')

        elif "=" in m[2] and "<=" not in m[2] and ">=" not in m[2]:
            whereCondition = 'EQ'
            whereConditionColumn = m[2].split('=')[0]
            whereConditionValue = m[2].split('=')[1].strip('"')
        elif "<" in m[2] and "=" not in m[2]:
            whereCondition = 'L'
            whereConditionColumn = m[2].split('<')[0]
            whereConditionValue = m[2].split('<')[1].strip('"')

        elif ">" in m[2] and "=" not in m[2]:
            whereCondition = 'G'
            whereConditionColumn = m[2].split('>')[0]
            whereConditionValue = m[2].split('>')[1].strip('"')

        elif "<" in m[2] and "=" in m[2]:
            whereCondition = 'LE'
            whereConditionColumn = m[2].split('<=')[0]
            whereConditionValue = m[2].split('<=')[1].strip('"')

        elif ">" in m[2] and "=" in m[2]:
            whereCondition = 'GE'
            whereConditionColumn = m[2].split('>=')[0]
            whereConditionValue = m[2].split('>=')[1].strip('"')

        else:
            print("wrong SQL Query")

        tableName=m[1]
        param = GDDObj.getDatabaseName(tableName)
        if param is None:
            print("Table does not exist")
            return
        DBMSName = param[0].getDBMSName()
        Database = param[1]
        if DBMSObj1.getDBMSName() == DBMSName:
            table = DBMSObj1.getTable(Database, tableName)
            if table is not None:
                return table.deleteExecution(tableName,whereCondition,whereConditionColumn,whereConditionValue)
        else:
            table = DBMSObj2.getTable(Database, tableName)
            if table is not None:
                return table.deleteExecution(tableName,whereCondition,whereConditionColumn,whereConditionValue)



# # Press the green button in the gutter to run the script.
# if __name__ == '__main__':
#     deleteParser('DELETE FROM user WHERE name!="parth"');