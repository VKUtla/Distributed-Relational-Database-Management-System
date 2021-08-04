from DistributedGCP import GCP

class Table:


    def __init__(self, tableName, columns, columnTypes, primaryKey):
        self.GCPObj = GCP()
        self.columns = columns
        self.tableName = tableName
        self.columnTypes = columnTypes
        self.data = []
        self.primaryKey = primaryKey
        self.lock = 0

    def insertData(self, dataItems):
        if self.lock == 1:
            print("Table is locked. Please try again")
            return False
        if self.primaryKey is not None:
            index = self.columns.index(self.primaryKey)
            dataRecords = dataItems.split(",")
            if len(self.columns) == len(dataRecords):
                dataRec = dataItems.replace(",", ":")
                if self.checkDuplicate(self.primaryKey, dataRec[index]):
                    self.data.append(dataRec)
                    counter = 0
                    for dataI in self.data:
                        dictI = {}
                        dataI = dataI.split(":")
                        if len(self.columns) != len(dataI):
                            continue
                        dataCounter = 0
                        for col in self.columns:
                            dictI.update({self.columns[dataCounter]:dataI[dataCounter]})
                            dataCounter = dataCounter + 1
                        newTable = self.tableName+'_'+str(counter+1)
                        self.GCPObj.writeDB(dictI,newTable)
                        counter = counter + 1
                    return True
        elif self.primaryKey is None:
            dataRecords = dataItems.split(",")
            if len(self.columns) == len(dataRecords):
                dataRec = data.replace(",", ":")
                self.data.append(dataRec)
                return True
        self.lock = 0

    def checkDuplicate(self, columnName, columnValue):
        index = self.columns.index(columnName)
        for dataRecord in self.data:
            dataInd = dataRecord.split(":")
            dataValue = dataInd[index]
            if dataValue == columnValue:
                return False
        return True

    def getColumns(self):
        return self.columns

    def getColumnTypes(self):
        return self.columnTypes

    def getTableName(self):
        return self.tableName

    def update(self, tableName, setConstraints, queryCondition, constraintType):
        if self.lock == 1:
            print("Table is locked. Please try again")
            return False

        self.lock = 1
        columnFound = False
        conditionMet = False
        columnCount = False
        listCols = []
        listVals = []
        key_var = []
        value_var = []
        if self.tableName != tableName:
            return False
        for key in queryCondition:
            key_var.append(key)
            value_var.append(queryCondition[key])
        if key_var is None or value_var is None or len(key_var) != len(value_var):
            return False
        for x in setConstraints:
            listCols.append(x)
            listVals.append(setConstraints.get(x))

        for x in listCols:
            columnCount = False
            for y in self.columns:
                if x == y:
                    columnCount = True
            if columnCount == False:
                print("Incorrrect columns")
                return False
        iter = -1
        for key in key_var:
            iter = iter + 1
            index = self.columns.index(key)
            if constraintType == 'equal':
                count = -1
                for dataRecord in self.data:
                    conditionMet = False
                    count = count + 1
                    dataItems = dataRecord.split(':')
                    value_curr = dataItems[index]
                    if value_curr == value_var[iter]:
                        conditionMet = True
                        updateCounter = 0
                        for x in listCols:
                            colInd = self.columns.index(x)
                            dataItems[colInd] = listVals[updateCounter]
                            updateCounter = updateCounter + 1
                    if conditionMet == True:
                        result = ''
                        count_elements = len(dataItems)
                        for p in dataItems:
                            result = result + p
                            if count_elements != 1:
                                result = result + ':'
                            count_elements = count_elements - 1
                        if self.primaryKey is not None:
                            duplicateFlag = False
                            dataUp = result.split(":")
                            valueToBeChecked = dataUp[self.columns.index(self.primaryKey)]
                            duplicateCounter = 0
                            for dataX in self.data:
                                if dataX[self.columns.index(self.primaryKey)] == valueToBeChecked:
                                    if duplicateCounter != 0:
                                        duplicateFlag = True
                                    duplicateCounter = duplicateCounter + 1
                            if duplicateFlag == False:
                                self.data[count] = result
                                counter = 0
                                for dataI in self.data:
                                    dictI = {}
                                    dataI = dataI.split(":")
                                    if len(self.columns) != len(dataI):
                                        continue
                                    dataCounter = 0
                                    for col in self.columns:
                                        dictI.update({self.columns[dataCounter]: dataI[dataCounter]})
                                        dataCounter = dataCounter + 1
                                    newTable = self.tableName + '_' + str(counter + 1)
                                    self.GCPObj.writeDB(dictI, newTable)
                                    counter = counter + 1
                        else:
                            self.data[count] = result
                            counter = 0
                            for dataI in self.data:
                                dictI = {}
                                dataI = dataI.split(":")
                                if len(self.columns) != len(dataI):
                                    continue
                                dataCounter = 0
                                for col in self.columns:
                                    dictI.update({self.columns[dataCounter]: dataI[dataCounter]})
                                    dataCounter = dataCounter + 1
                                newTable = self.tableName + '_' + str(counter + 1)
                                self.GCPObj.writeDB(dictI, newTable)
                                counter = counter + 1
            elif constraintType == 'greaterEqual':
                count = -1
                for dataRecord in self.data:
                    conditionMet = False
                    count = count + 1
                    dataItems = dataRecord.split(':')
                    value_curr = dataItems[index]
                    if value_curr >= value_var[iter]:
                        conditionMet = True
                        updateCounter = 0
                        for x in listCols:
                            colInd = self.columns.index(x)
                            dataItems[colInd] = listVals[updateCounter]
                            updateCounter = updateCounter + 1
                    if conditionMet == True:
                        result = ''
                        count_elements = len(dataItems)
                        for p in dataItems:
                            result = result + p
                            if count_elements != 1:
                                result = result + ':'
                            count_elements = count_elements - 1
                        if self.primaryKey is not None:
                            duplicateFlag = False
                            dataUp = result.split(":")
                            valueToBeChecked = dataUp[self.columns.index(self.primaryKey)]
                            duplicateCounter = 0
                            for dataX in self.data:
                                if dataX[self.columns.index(self.primaryKey)] == valueToBeChecked:
                                    if duplicateCounter != 0:
                                        duplicateFlag = True
                                    duplicateCounter = duplicateCounter + 1
                            if duplicateFlag == False:
                                self.data[count] = result
                                counter = 0
                                for dataI in self.data:
                                    dictI = {}
                                    dataI = dataI.split(":")
                                    if len(self.columns) != len(dataI):
                                        continue
                                    dataCounter = 0
                                    for col in self.columns:
                                        dictI.update({self.columns[dataCounter]: dataI[dataCounter]})
                                        dataCounter = dataCounter + 1
                                    newTable = self.tableName + '_' + str(counter + 1)
                                    self.GCPObj.writeDB(dictI, newTable)
                                    counter = counter + 1
                        else:
                            self.data[count] = result
                            counter = 0
                            for dataI in self.data:
                                dictI = {}
                                dataI = dataI.split(":")
                                if len(self.columns) != len(dataI):
                                    continue
                                dataCounter = 0
                                for col in self.columns:
                                    dictI.update({self.columns[dataCounter]: dataI[dataCounter]})
                                    dataCounter = dataCounter + 1
                                newTable = self.tableName + '_' + str(counter + 1)
                                self.GCPObj.writeDB(dictI, newTable)
                                counter = counter + 1
            elif constraintType == 'lesserEqual':
                count = -1
                for dataRecord in self.data:
                    conditionMet = False
                    count = count + 1
                    dataItems = dataRecord.split(':')
                    value_curr = dataItems[index]
                    if value_curr <= value_var[iter]:
                        conditionMet = True
                        updateCounter = 0
                        for x in listCols:
                            colInd = self.columns.index(x)
                            dataItems[colInd] = listVals[updateCounter]
                            updateCounter = updateCounter + 1
                    if conditionMet == True:
                        result = ''
                        count_elements = len(dataItems)
                        for p in dataItems:
                            result = result + p
                            if count_elements != 1:
                                result = result + ':'
                            count_elements = count_elements - 1
                        if self.primaryKey is not None:
                            duplicateFlag = False
                            dataUp = result.split(":")
                            valueToBeChecked = dataUp[self.columns.index(self.primaryKey)]
                            duplicateCounter = 0
                            for dataX in self.data:
                                if dataX[self.columns.index(self.primaryKey)] == valueToBeChecked:
                                    if duplicateCounter != 0:
                                        duplicateFlag = True
                                    duplicateCounter = duplicateCounter + 1
                            if duplicateFlag == False:
                                self.data[count] = result
                                counter = 0
                                for dataI in self.data:
                                    dictI = {}
                                    dataI = dataI.split(":")
                                    if len(self.columns) != len(dataI):
                                        continue
                                    dataCounter = 0
                                    for col in self.columns:
                                        dictI.update({self.columns[dataCounter]: dataI[dataCounter]})
                                        dataCounter = dataCounter + 1
                                    newTable = self.tableName + '_' + str(counter + 1)
                                    self.GCPObj.writeDB(dictI, newTable)
                                    counter = counter + 1
                        else:
                            self.data[count] = result
                            counter = 0
                            for dataI in self.data:
                                dictI = {}
                                dataI = dataI.split(":")
                                if len(self.columns) != len(dataI):
                                    continue
                                dataCounter = 0
                                for col in self.columns:
                                    dictI.update({self.columns[dataCounter]: dataI[dataCounter]})
                                    dataCounter = dataCounter + 1
                                newTable = self.tableName + '_' + str(counter + 1)
                                self.GCPObj.writeDB(dictI, newTable)
                                counter = counter + 1
            elif constraintType == 'greaterThan':
                count = -1
                for dataRecord in self.data:
                    conditionMet = False
                    count = count + 1
                    dataItems = dataRecord.split(':')
                    value_curr = dataItems[index]
                    if value_curr > value_var[iter]:
                        conditionMet = True
                        updateCounter = 0
                        for x in listCols:
                            colInd = self.columns.index(x)
                            dataItems[colInd] = listVals[updateCounter]
                            updateCounter = updateCounter + 1
                    if conditionMet == True:
                        result = ''
                        count_elements = len(dataItems)
                        for p in dataItems:
                            result = result + p
                            if count_elements != 1:
                                result = result + ':'
                            count_elements = count_elements - 1
                        if self.primaryKey is not None:
                            duplicateFlag = False
                            dataUp = result.split(":")
                            valueToBeChecked = dataUp[self.columns.index(self.primaryKey)]
                            duplicateCounter = 0
                            for dataX in self.data:
                                if dataX[self.columns.index(self.primaryKey)] == valueToBeChecked:
                                    if duplicateCounter != 0:
                                        duplicateFlag = True
                                    duplicateCounter = duplicateCounter + 1
                            if duplicateFlag == False:
                                self.data[count] = result
                                counter = 0
                                for dataI in self.data:
                                    dictI = {}
                                    dataI = dataI.split(":")
                                    if len(self.columns) != len(dataI):
                                        continue
                                    dataCounter = 0
                                    for col in self.columns:
                                        dictI.update({self.columns[dataCounter]: dataI[dataCounter]})
                                        dataCounter = dataCounter + 1
                                    newTable = self.tableName + '_' + str(counter + 1)
                                    self.GCPObj.writeDB(dictI, newTable)
                                    counter = counter + 1
                        else:
                            self.data[count] = result
                            counter = 0
                            for dataI in self.data:
                                dictI = {}
                                dataI = dataI.split(":")
                                if len(self.columns) != len(dataI):
                                    continue
                                dataCounter = 0
                                for col in self.columns:
                                    dictI.update({self.columns[dataCounter]: dataI[dataCounter]})
                                    dataCounter = dataCounter + 1
                                newTable = self.tableName + '_' + str(counter + 1)
                                self.GCPObj.writeDB(dictI, newTable)
                                counter = counter + 1
            elif constraintType == 'lessThan':
                count = -1
                for dataRecord in self.data:
                    conditionMet = False
                    count = count + 1
                    dataItems = dataRecord.split(':')
                    value_curr = dataItems[index]
                    if value_curr < value_var[iter]:
                        conditionMet = True
                        updateCounter = 0
                        for x in listCols:
                            colInd = self.columns.index(x)
                            dataItems[colInd] = listVals[colInd]
                            updateCounter = updateCounter + 1
                    if conditionMet == True:
                        result = ''
                        count_elements = len(dataItems)
                        for p in dataItems:
                            result = result + p
                            if count_elements != 1:
                                result = result + ':'
                            count_elements = count_elements - 1
                        if self.primaryKey is not None:
                            duplicateFlag = False
                            dataUp = result.split(":")
                            valueToBeChecked = dataUp[self.columns.index(self.primaryKey)]
                            duplicateCounter = 0
                            for dataX in self.data:
                                if dataX[self.columns.index(self.primaryKey)] == valueToBeChecked:
                                    if duplicateCounter != 0:
                                        duplicateFlag = True
                                    duplicateCounter = duplicateCounter + 1
                            if duplicateFlag == False:
                                self.data[count] = result
                                counter = 0
                                for dataI in self.data:
                                    dictI = {}
                                    dataI = dataI.split(":")
                                    if len(self.columns) != len(dataI):
                                        continue
                                    dataCounter = 0
                                    for col in self.columns:
                                        dictI.update({self.columns[dataCounter]: dataI[dataCounter]})
                                        dataCounter = dataCounter + 1
                                    newTable = self.tableName + '_' + str(counter + 1)
                                    self.GCPObj.writeDB(dictI, newTable)
                                    counter = counter + 1
                        else:
                            self.data[count] = result
                            counter = 0
                            for dataI in self.data:
                                dictI = {}
                                dataI = dataI.split(":")
                                if len(self.columns) != len(dataI):
                                    continue
                                dataCounter = 0
                                for col in self.columns:
                                    dictI.update({self.columns[dataCounter]: dataI[dataCounter]})
                                    dataCounter = dataCounter + 1
                                newTable = self.tableName + '_' + str(counter + 1)
                                self.GCPObj.writeDB(dictI, newTable)
                                counter = counter + 1
        self.lock = 0
        return True

    def deleteExecution(self, tableName, whereCondition, whereConditionColumn, whereConditionValue):
        if self.lock == 1:
            print("Table is locked. Please try again")
            return False
        if self.tableName != tableName:
            return False
        index = self.columns.index(whereConditionColumn)
        list=[]
        if whereCondition == 'EQ':
            dataCounter = 1
            for dataRecord in self.data:
                data = dataRecord.split(':')
                tableColumnValue = data[index]
                if tableColumnValue == whereConditionValue:
                    list.append(dataRecord)
                    newTable = self.tableName+'_'+str(dataCounter)
                    self.GCPObj.deleteData(newTable)
                dataCounter = dataCounter + 1
            for dataRecord in list:
                self.data.remove(dataRecord)

        elif whereCondition == 'L':
            dataCounter = 1
            for dataRecord in self.data:
                data = dataRecord.split(':')
                tableColumnValue = data[index]
                if tableColumnValue < whereConditionValue:
                    list.append(dataRecord)
                    newTable = self.tableName + '_' + str(dataCounter)
                    self.GCPObj.deleteData(newTable)
                dataCounter = dataCounter + 1
            for dataRecord in list:
                self.data.remove(dataRecord)

        elif whereCondition == 'G':
            dataCounter = 1
            for dataRecord in self.data:
                data = dataRecord.split(':')
                tableColumnValue = data[index]
                if tableColumnValue > whereConditionValue:
                    list.append(dataRecord)
                    newTable = self.tableName + '_' + str(dataCounter)
                    self.GCPObj.deleteData(newTable)
                dataCounter = dataCounter + 1
            for dataRecord in list:
                self.data.remove(dataRecord)

        elif whereCondition == 'LE':
            dataCounter = 1
            for dataRecord in self.data:
                data = dataRecord.split(':')
                tableColumnValue = data[index]
                if tableColumnValue <= whereConditionValue:
                    list.append(dataRecord)
                    newTable = self.tableName + '_' + str(dataCounter)
                    self.GCPObj.deleteData(newTable)
                dataCounter = dataCounter + 1
            for dataRecord in list:
                self.data.remove(dataRecord)

        elif whereCondition == 'GE':
            dataCounter = 1
            for dataRecord in self.data:
                data = dataRecord.split(':')
                tableColumnValue = data[index]
                if tableColumnValue >= whereConditionValue:
                    list.append(dataRecord)
                    newTable = self.tableName + '_' + str(dataCounter)
                    self.GCPObj.deleteData(newTable)
                dataCounter = dataCounter + 1
            for dataRecord in list:
                self.data.remove(dataRecord)

        elif whereCondition == 'NE':
            dataCounter = 1
            for dataRecord in self.data:
                data = dataRecord.split(':')
                tableColumnValue = data[index]
                if tableColumnValue != whereConditionValue:
                    list.append(dataRecord)
                    newTable = self.tableName + '_' + str(dataCounter)
                    self.GCPObj.deleteData(newTable)
                dataCounter = dataCounter + 1
            for dataRecord in list:
                self.data.remove(dataRecord)
        else:
            print("no record deleted")
            return False
        for dataRecord in self.data:
            print(dataRecord)
        print("record deleted")
        self.lock = 0
        return True

    def selectExecution(self, tableName, column, whereCondition, whereConditionColumn, whereConditionValue):
        if self.lock == 1:
            print("Table is locked. Please try again")
            return False
        list = []
        recordFound = False
        if self.tableName != tableName:
            return False
        if column=='ALL':
            if whereCondition=='':
                for dataRecord in self.data:
                    list.append(dataRecord)
                for dataRecord in list:
                    print(dataRecord)
                self.lock = 0
                return
            index =self.columns.index(whereConditionColumn)

        else:
            index = self.columns.index(whereConditionColumn)
            displayIndex =[]
            for c in column:
                columnDisplay = column.split(',')

            for n,i in enumerate(columnDisplay):
                displayIndex.append(self.columns.index(i))



        if whereCondition == 'EQ':
            for dataRecord in self.data:
                data = dataRecord.split(':')
                tableColumnValue = data[index]
                if tableColumnValue == whereConditionValue:
                    recordFound = True
                    if (column == 'ALL'):
                        list.append(dataRecord)
                    else:
                        for i in displayIndex:
                            list.append(data[i])

        elif whereCondition == 'L':
            for dataRecord in self.data:
                data = dataRecord.split(':')
                tableColumnValue = data[index]
                if tableColumnValue < whereConditionValue:
                    recordFound = True
                    if (column == 'ALL'):
                        list.append(dataRecord)
                    else:
                        for i in displayIndex:
                            list.append(data[i])

        elif whereCondition == 'G':
            for dataRecord in self.data:
                data = dataRecord.split(':')
                tableColumnValue = data[index]
                if tableColumnValue > whereConditionValue:
                    recordFound = True
                    if (column == 'ALL'):
                        list.append(dataRecord)
                    else:
                        for i in displayIndex:
                            list.append(data[i])

        elif whereCondition == 'LE':
            for dataRecord in self.data:
                data = dataRecord.split(':')
                tableColumnValue = data[index]
                if tableColumnValue <= whereConditionValue:
                    recordFound = True
                    if (column == 'ALL'):
                        list.append(dataRecord)
                    else:
                        for i in displayIndex:
                            list.append(data[i])

        elif whereCondition == 'GE':
            for dataRecord in self.data:
                data = dataRecord.split(':')
                tableColumnValue = data[index]
                if tableColumnValue >= whereConditionValue:
                    recordFound = True
                    if (column == 'ALL'):
                        list.append(dataRecord)
                    else:
                        for i in displayIndex:
                            list.append(data[i])

        elif whereCondition == 'NE':
            for dataRecord in self.data:
                data = dataRecord.split(':')
                tableColumnValue = data[index]
                if tableColumnValue != whereConditionValue:
                    recordFound = True
                    if (column == 'ALL'):
                        list.append(dataRecord)
                    else:
                        for i in displayIndex:
                            list.append(data[i])

        else:
            print("Please enter valid where condition")

        if recordFound==False:
            print("no record found")

        for dataRecord in list:
            print(dataRecord)
        self.lock = 0
        return list