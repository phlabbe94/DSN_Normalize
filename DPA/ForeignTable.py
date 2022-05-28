# Copyright Philippe Labbe 2022 - Licence MIT

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from pymongo.errors import ExecutionTimeout


class ForeignTable:

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

    def setKeysTable(self):

        # Liste des Entités
        liste_entites = self.dsa_liens.find({}).distinct('code_bloc')

        for entite in liste_entites:

            # Table à alimenter
            keys_table = 'KEYS_' + entite

            # Drop table de clés
            self.dpa_base[keys_table].drop()

            coll_dep = self.dpa_base[entite]

            print('Foreign Keys - ' + keys_table + ' - Début')
            for x in coll_dep.find({}).distinct('pk'):
                pk = x
                enreg = {"pk": pk}
                coll = self.dpa_base[entite]
                test = True

                while test:

                    fk_name = coll.find_one({"pk": pk})['fk']['fk_name']
                    fk_value = coll.find_one({"pk": pk})['fk']['fk_value']
                    if fk_name != "":

                        enreg["FK_" + fk_name] = fk_value
                        coll = self.dpa_base[fk_name]
                        pk = fk_value

                    else:

                        test = False

                self.setDatabase(keys_table, enreg)

            # Création index
            self.dpa_base[keys_table].create_index('pk')
            print('Foreign Keys - ' + keys_table + ' - Fin')
