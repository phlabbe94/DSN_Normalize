# Copyright Philippe Labbe 2022 - Licence MIT

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from pymongo.errors import ExecutionTimeout


class Entity:

    def __init__(self, urlMongo, portMongo, dsa, dpa):

        # Connexion MongoDB
        client_dsn = MongoClient(urlMongo, portMongo)

        # Base DSA
        self.dsa_base = client_dsn[dsa]

        # Base DPA
        self.dpa_base = client_dsn[dpa]

        # Collection Liens
        self.dsa_liens = self.dsa_base['liens']

    def setDatabase(self, table, enreg):

        try:

            self.dpa_base[table].insert_one(enreg)

        except ConnectionFailure:

            print("Base MongoDB non accessible")

        except ExecutionTimeout:

            print("Insertion MongoDB en timeout")

    def getValue(self, pk, bloc):

        try:

            value = self.dsa_base['dsa_relational'].find({"pk": pk, "bloc": bloc})

        except:

            value = ""

        return value

    def getListPk(self, coll):

        try:

            liste_pk = self.dsa_base['dsa_relational'].find({'bloc': coll}).distinct('pk')

        except:

            liste_pk = ""

        return liste_pk

    def setEntity(self):

        # Liste des Entités
        liste_entites = self.dsa_liens.find({}).distinct('code_bloc')

        for entite in liste_entites:

            # Drop entité
            self.dpa_base[entite].drop()

            liste_pk = self.getListPk(entite)

            print('Entités - ' + entite + ' - Début')
            for pk in liste_pk:

                enreg = {"pk": pk, "type": self.dsa_liens.find_one({'code_bloc': entite})['lib_bloc']}
                value_entite = self.getValue(pk, entite)
                fk = ""
                date_staging = ''

                for x in value_entite:

                    enreg[x['rubrique']] = x['valeur']
                    fk = x['fk']
                    date_staging = x['date_staging']

                enreg['date_staging'] = date_staging
                enreg['fk'] = fk

                self.setDatabase(entite, enreg)

            # Création index
            self.dpa_base[entite].create_index('pk')

            print('Entités - ' + entite + ' - Fin')
