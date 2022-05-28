# Copyright Philippe Labbe 2021 - Licence MIT

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from pymongo.errors import ExecutionTimeout
from datetime import datetime
import csv


class Staging:

    def __init__(self, pathFileDsn, urlMongo, portMongo, dsa):

        # Fichier DSN
        self.pathFileDsn = pathFileDsn

        # Connexion MongoDB
        client_dsn = MongoClient(urlMongo, portMongo)

        # Base DSA
        dsa_base = client_dsn[dsa]

        # Collection Staging
        self.dsa_staging = dsa_base.dsa_staging

        # Date staging
        self.date_staging = datetime.now()

    def setStaging(self):

        try:

            # Drop Staging
            self.dsa_staging.drop()

            # Lecture du fichier DSN
            with open(self.pathFileDsn) as csv_dsn:

                csv_buffer = csv.reader(csv_dsn, delimiter=",", quotechar="'")

                for row in csv_buffer:

                    rubrique = row[0]
                    valeur = row[1]
                    bloc = row[0][0:10]

                    enreg = {"rubrique": rubrique,
                             "valeur": valeur,
                             "bloc": bloc,
                             "date_staging": self.date_staging}

                    self.dsa_staging.insert_one(enreg)

        except FileNotFoundError:

            print("Lecture du fichier impossible")

        except ConnectionFailure:

            print("Base MongoDB non accessible")

        except ExecutionTimeout:

            print("Insertion MongoDB en timeout")

    def getStaging(self):

        cursor_staging = ""

        try:

            # Lecture DSA - Staging
            cursor_staging = self.dsa_staging.find({})

        except ConnectionFailure:

            print("Base MongoDB non accessible")

        except ExecutionTimeout:

            print("Lecture MongoDB en timeout")

        return cursor_staging
