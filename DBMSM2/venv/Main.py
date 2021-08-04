import logging
from DeleteParser import DeleteParser
from GDD import GDD
from DBMS import DBMS
from InsertParser import InsertParser
from SelectParser import SelectParser
from Table import Table
from UpdateParser import UpdateParser
from CreateParser import CreateParser

def addTable(DBMS):
    cols = ["id", "name"]
    colTypes = ["string", "string"]
    table1 = Table("demoTable", cols, colTypes)

    DBMS.addTable("customer", table1)
    insertData(table1)

def getDBMS1():
    DBMS1 = DBMS("local")
    DBMS1.addDatabase("customer");
    addTable(DBMS1)
    return DBMS1

def getDBMS2():
    DBMS2 = DBMS("remote")
    return DBMS2

def getGDDObj():
    GDDObj = GDD()
    GDDObj.addRecord("customer", "local", "demoTable");
    return GDDObj

def insertData(table):
    table.insertData('456,vamsi')
    table.insertData('123,parth')
    table.insertData('123,karishma')

def mapQuery(query, databaseName, GDD, DBMSLocal, DBMSRemote):

    if "select" in query:
        se = SelectParser()
        list = se.selectParser(query, DBMSLocal, DBMSRemote, GDD)
        #print(list)

    elif "update" in query:
        up = UpdateParser()
        up.updateParser(query, DBMSLocal, DBMSRemote, GDD)

    elif "delete" in query:
        de = DeleteParser()
        de.deleteParser(query, DBMSLocal, DBMSRemote, GDD)

    elif "insert" in query:
        ip = InsertParser()
        ip.insertParser(query, DBMSLocal, DBMSRemote, GDD)

    elif "create" in query:
        cr = CreateParser()
        if cr.createParser(query, GDD, databaseName, DBMSLocal, DBMSRemote):
            print("Table created successfully in ", databaseName)
        else:
            print("Unable to create table in ", databaseName)
    return

def authenticate(username, password):
    fileData = open("credentials.txt", "r")
    for record in fileData:
        recordData = record.split(":")
        if recordData[0] == username and recordData[1].rstrip("\n") == password and recordData.__len__()!=0:
            fileData.close()
            return True
    fileData.close()
    return False

def addUser(username, password):
    fileData = open("credentials.txt", "r")
    for record in fileData:
        recordData = record.split(":")
        if recordData[0] == username:
            print("Username already exists")
            fileData.close()
            return False
    fileData.close()
    fileData = open("credentials.txt", "a")
    data = username + ':' + password + "\n"
    fileData.write(data)
    fileData.close()
    return True

def reloadData(GDD, DBMSLocal, DBMSRemote):
    databaseOption = 1
    currentDatabaseName = ''
    transactions = list()
    transactionFlag = False
    fileData = open("appQueries.log", "r")
    for query in fileData:
        if "start" in query:
            if len(currentDatabaseName) != 0:
                transactionFlag = True
        elif "create" in query:
            if transactionFlag == True and len(currentDatabaseName) != 0:
                transactions.append(query)
            else:
                data = query.split(" ")
                if len(data) == 2:
                    if databaseOption == 1:
                        GDD.addDatabaseAndDBMS(data[1], DBMSLocal)
                        DBMSLocal.addDatabase(data[1])
                        databaseOption = databaseOption + 1
                    else:
                        GDD.addDatabaseAndDBMS(data[1], DBMSRemote)
                        DBMSRemote.addDatabase(data[1])
                        databaseOption = 1
                else:
                    if len(currentDatabaseName) != 0:
                        mapQuery(query, currentDatabaseName, GDD, DBMSLocal, DBMSRemote)

        elif "drop" in query:
            if transactionFlag == True and len(currentDatabaseName) != 0:
                transactions.append(query)
            else:
                if len(currentDatabaseName) != 0:
                    queryItems = query.split(" ")
                    if queryItems[1] == "database":
                        if GDD.getDBMS(queryItems[2]) == 'local':
                            DBMSLocal.deleteDatabase(queryItems[2])
                            GDD.removeRecordDatabase(queryItems[2], 'local')
                        elif GDD.getDBMS(queryItems[2]) == 'remote':
                            DBMSRemote.deleteDatabase(queryItems[2])
                            GDD.removeRecordDatabase(queryItems[2], 'remote')
                    elif queryItems[1] == "table":
                        result = GDD.getDatabaseName(queryItems[2])
                        if result is None:
                            print()
                        elif result[0].getDBMSName() == 'local':
                            GDD.removeRecord(result[1], result[0], queryItems[2])
                            DBMSLocal.deleteTable(result[1], queryItems[2])
                        elif result[0].getDBMSName() == 'remote':
                            GDD.removeRecord(result[1], result[0], queryItems[2])
                            DBMSRemote.deleteTable(result[1], queryItems[2])

        elif "use" in query:
            if transactionFlag == True and len(currentDatabaseName) != 0:
                transactions.append(query)
            else:
                databaseName = query.split(" ")
                currentDatabaseName = databaseName[1]
        elif "commit" in query:
            if len(currentDatabaseName) != 0:
                for transactionQuery in transactions:
                    mapQuery(transactionQuery, currentDatabaseName, GDD, DBMSLocal, DBMSRemote)
                transactionFlag = False
                transactions.clear()
        elif "rollback" in query:
            if len(currentDatabaseName) != 0:
                transactionFlag = False
                transactions.clear()
        else:
            if transactionFlag == True and len(currentDatabaseName) != 0:
                transactions.append(query)
            else:
                if len(currentDatabaseName) != 0:
                    mapQuery(query, currentDatabaseName, GDD, DBMSLocal, DBMSRemote)


def encrypt(password):
    finalResult = ''
    for letter in password:
        finalResult = finalResult + str(ord(letter)+1)
    return finalResult

def login():
    authenticationFlag = False
    while authenticationFlag == False:
        inputChoice = input("Input 1 to login and 2 to register: ")
        username = input("Enter username: ")
        password = input("Enter password: ")
        if int(inputChoice) == 1:
            password = encrypt(password)
            authenticationFlag = authenticate(username, password)
            if authenticationFlag == False:
                print("Invalid username and password. Please try again\n")
        elif int(inputChoice) == 2:
            password = encrypt(password)
            addUser(username, password)
        else:
            print("Invalid input.")

    return True

if __name__ == '__main__':

    authenticationFlag = False
    updateQuery = 'update demoTable set id=123 where name=vamsi'
    deleteQuery = 'delete from demoTable where id<=123'
    transactionFlag = False
    transactions = list()
    databaseOption = 1
    GDD = GDD()
    DBMSLocal = DBMS("local")
    DBMSRemote = DBMS("remote")
    currentDatabaseName = ''
    # query = 'select id,name from demoTable where id<=456'

    authenticationFlag = login()

    if authenticationFlag == False:
        exit(0)
    print("Login successful")

    reloadData(GDD, DBMSLocal, DBMSRemote)
    print("Data restored to previous state.")

    logging.basicConfig(filename="appQueries.log", filemode='a', level=logging.INFO, format="%(message)s")

    while True:

        query = input("Enter query:")
        logging.info(query)
        if "exit" in query:
            exit()

        elif "start" in query:
            if len(currentDatabaseName) != 0:
                transactionFlag = True
            else:
                print("No database selected.")

        elif "commit" in query:
            if len(currentDatabaseName) != 0:
                for transactionQuery in transactions:
                    mapQuery(transactionQuery, currentDatabaseName, GDD, DBMSLocal, DBMSRemote)
                transactionFlag = False
                transactions.clear()
            else:
                print("No database selected.")

        elif "rollback" in query:
            if len(currentDatabaseName) != 0:
                transactionFlag = False
                transactions.clear()
            else:
                print("No database selected.")

        elif "create" in query:
            if transactionFlag == True and len(currentDatabaseName) != 0:
                transactions.append(query)
            else:
                data = query.split(" ")
                if len(data) == 2:
                    if databaseOption == 1:
                        GDD.addDatabaseAndDBMS(data[1], DBMSLocal)
                        DBMSLocal.addDatabase(data[1])
                        databaseOption = databaseOption + 1
                    else:
                        GDD.addDatabaseAndDBMS(data[1], DBMSRemote)
                        DBMSRemote.addDatabase(data[1])
                        databaseOption = 1
                else:
                    if len(currentDatabaseName) != 0:
                        mapQuery(query, currentDatabaseName, GDD, DBMSLocal, DBMSRemote)
                    else:
                        print("No database selected.")

        elif "drop" in query:
            if transactionFlag == True and len(currentDatabaseName) != 0:
                transactions.append(query)
            else:
                queryItems = query.split(" ")
                if queryItems[1] == "database":
                    if GDD.getDBMS(queryItems[2]).getDBMSName() == 'local':
                        if DBMSLocal.deleteDatabase(queryItems[2]) and GDD.removeRecordDatabase(queryItems[2], 'local'):
                            print("Database deleted successfully")
                        else:
                            print("Database deletion was unsuccessful")
                    elif GDD.getDBMS(queryItems[2]).getDBMSName() == 'remote':
                        if DBMSRemote.deleteDatabase(queryItems[2]) and GDD.removeRecordDatabase(queryItems[2], 'remote'):
                            print("Database deleted successfully")
                        else:
                            print("Database deletion was unsuccessful")
                elif queryItems[1] == "table":
                    if len(currentDatabaseName) != 0:
                        result = GDD.getDatabaseName(queryItems[2])
                        if result is None:
                            print('Table does not exist')
                        elif result[0].getDBMSName() == 'local':
                            if GDD.removeRecord(result[1], result[0], queryItems[2]) and DBMSLocal.deleteTable(result[1], queryItems[2]):
                                print("Table deleted successfully")
                        elif result[0].getDBMSName() == 'remote':
                            if GDD.removeRecord(result[1], result[0], queryItems[2]) and DBMSRemote.deleteTable(result[1], queryItems[2]):
                                print("Table deleted successfully")
                    else:
                        print("No database selected.")

        elif "use" in query:
            if transactionFlag == True and len(currentDatabaseName) != 0:
                transactions.append(query)
            else:
                databaseName = query.split(" ")
                currentDatabaseName = databaseName[1]

        else:
            if transactionFlag == True and len(currentDatabaseName) != 0:
                transactions.append(query)
            else:
                if len(currentDatabaseName) != 0:
                    mapQuery(query, currentDatabaseName, GDD, DBMSLocal, DBMSRemote)
                else:
                    print("No database selected.")
