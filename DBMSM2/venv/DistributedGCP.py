from google.cloud import firestore
from google.oauth2 import service_account

class GCP:
    def writeDB(self,data,tablename):
        credentials = service_account.Credentials.from_service_account_file(
            "C:\\Users\\USER\\Downloads\\csci-5408-w21-304616-2523bd2e0d44.json")
        scoped_credentials = credentials.with_scopes(
            ['https://www.googleapis.com/auth/cloud-platform'])
        db = firestore.Client(credentials=credentials)

        users_ref = db.collection(u'database').document(tablename)
        if users_ref:
            users_ref.delete()

        doc_ref = db.collection(u'database').document(tablename)
        doc_ref.set(data)

    def readData(self,tablename):
        credentials = service_account.Credentials.from_service_account_file(
            "C:\\Users\\USER\\Downloads\\csci-5408-w21-304616-2523bd2e0d44.json")
        scoped_credentials = credentials.with_scopes(
            ['https://www.googleapis.com/auth/cloud-platform'])
        db = firestore.Client(credentials=credentials)

        test_ref = db.collection(u'database').document(tablename).get()
        return test_ref

    def deleteData(self, tablename):
        credentials = service_account.Credentials.from_service_account_file(
            "C:\\Users\\USER\\Downloads\\csci-5408-w21-304616-2523bd2e0d44.json")
        scoped_credentials = credentials.with_scopes(
            ['https://www.googleapis.com/auth/cloud-platform'])
        db = firestore.Client(credentials=credentials)

        users_ref = db.collection(u'database').document(tablename)
        if users_ref:
            users_ref.delete()