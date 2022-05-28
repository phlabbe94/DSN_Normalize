# Copyright Philippe Labbe 2021 - Licence MIT

from pymongo import MongoClient, ASCENDING
from pymongo.errors import ConnectionFailure
from pymongo.errors import ExecutionTimeout
from pymongo.cursor import Cursor
import uuid


class Relational:

    def __init__(self, urlMongo, portMongo, dsa):

        # Connexion MongoDB
        client_dsn = MongoClient(urlMongo, portMongo)

        # Base DSA
        dsa_base = client_dsn[dsa]

        # Collection Structure
        self.dsa_structure = dsa_base['structure']

        # Collection Liens
        self.dsa_liens = dsa_base['liens']

        # Collection Relational
        self.dsa_relational = dsa_base['dsa_relational']

    def setDatabase(self, enreg):

        try:

            self.dsa_relational.insert_one(enreg)

        except ConnectionFailure:

            print("Base MongoDB non accessible")

        except ExecutionTimeout:

            print("Insertion MongoDB en timeout")

    def setRelational(self, staging: Cursor):

        # Drop Relational
        self.dsa_relational.drop()

        # Initialisation valeurs précédentes
        rubrique_precedente = ''
        bloc_precedent = ''

        # Dictionnaire des clés primaires
        pk = {}

        for ligne in staging:

            # Lecture staging
            rubrique = ligne['rubrique']
            valeur = ligne['valeur']
            bloc = ligne['bloc']
            date_staging = ligne['date_staging']

            if bloc_precedent != bloc or rubrique_precedente > rubrique:

                pk[bloc] = str(uuid.uuid4())

            type_bloc = self.dsa_structure.find_one({'code_rubrique': rubrique})['lib_bloc']

            if type_bloc:

                fk_name = self.dsa_liens.find_one({"code_bloc": bloc})['fk']
                fk_value = ''

                if fk_name != '':

                    fk_value = pk[fk_name]

                enreg = {"pk": pk[bloc],
                         "type": type_bloc,
                         "rubrique": rubrique,
                         "bloc": bloc,
                         "valeur": valeur,
                         "date_staging": date_staging,
                         "fk": {"fk_name": fk_name,
                                "fk_value": fk_value}}

                self.setDatabase(enreg)

            rubrique_precedente = rubrique
            bloc_precedent = bloc

        self.dsa_relational.create_index([('pk', ASCENDING), ('bloc', ASCENDING)])
