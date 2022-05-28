# Copyright Philippe Labbe 2022 - Licence MIT

from pymongo import MongoClient, ASCENDING
from pymongo.errors import ConnectionFailure
from pymongo.errors import ExecutionTimeout
import csv


class Referentiel:

    def __init__(self, urlMongo, portMongo, dsa):

        # Connexion MongoDB
        client_dsn = MongoClient(urlMongo, portMongo)

        # Base DSA
        dsa_base = client_dsn[dsa]

        # Collection Structure
        self.dsa_structure = dsa_base['structure']

        # Collection Liens
        self.dsa_liens = dsa_base['liens']

    def setStructure(self, pathFileStructure):

        try:

            # Drop structure
            self.dsa_structure.drop()

            # Lecture referentiel Structure
            with open(pathFileStructure) as csv_structure:

                csv_buffer = csv.reader(csv_structure, delimiter=',', quotechar='"')

                for row in csv_buffer:

                    code_bloc = row[0]
                    lib_bloc = row[1]
                    code_rubrique = row[2]
                    lib_rubrique = row[3]

                    enreg = {"code_bloc": code_bloc,
                             "lib_bloc": lib_bloc,
                             "code_rubrique": code_rubrique,
                             "lib_rubrique": lib_rubrique}

                    self.dsa_structure.insert_one(enreg)

                self.dsa_structure.create_index([('code_rubrique', ASCENDING)])

        except FileNotFoundError:

            print("Lecture du fichier impossible")

        except ConnectionFailure:

            print("Base MongoDB non accessible")

        except ExecutionTimeout:

            print("Insertion MongoDB en timeout")

    def setLiens(self, pathFileLiens):

        try:

            # Drop Liens
            self.dsa_liens.drop()

            # Lecture referentiel Structure
            with open(pathFileLiens) as csv_liens:

                csv_buffer = csv.reader(csv_liens, delimiter=',', quotechar='"')

                for row in csv_buffer:

                    code_bloc = row[0]
                    lib_bloc = row[1]
                    fk = row[2]

                    enreg = {"code_bloc": code_bloc,
                             "lib_bloc": lib_bloc,
                             "fk": fk}

                    self.dsa_liens.insert_one(enreg)

                self.dsa_liens.create_index([('code_bloc', ASCENDING)])

        except FileNotFoundError:

            print("Lecture du fichier impossible")

        except ConnectionFailure:

            print("Base MongoDB non accessible")

        except ExecutionTimeout:

            print("Insertion MongoDB en timeout")
