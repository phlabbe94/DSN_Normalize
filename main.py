# Copyright Philippe Labbe 2021 - Licence MIT

from DSA.Staging import Staging
from DSA.Relational import Relational
from DPA.Entity import Entity
from DPA.ForeignTable import ForeignTable
from DSA.Referentiel import Referentiel
import argparse


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--chemin", help="Chemin fichier DSN", required=True)
    parser.add_argument("-u", "--url_mongo", help="URL serveur MongoDB", required=True)
    parser.add_argument("-p", "--port_mongo", help="Port serveur MongoDB", required=True, type=int)
    parser.add_argument("-s", "--dsa_base", help="Base DSA", required=True)
    parser.add_argument("-t", "--dpa_base", help="Base DPA", required=True)
    parser.add_argument("-m", "--dmt_base", help="Base DMT", required=True)

    args = parser.parse_args()
    dsn_file = args.chemin
    urlMongo = args.url_mongo
    portMongo = args.port_mongo
    base_dsa = args.dsa_base
    base_dpa = args.dpa_base
    base_dmt = args.dmt_base

    # Chargement référentiel Structure
    ref = Referentiel(urlMongo, portMongo, base_dsa)
    ref.setStructure('Referentiel/Structure.csv')
    ref.setLiens('Referentiel/Liens.csv')

    print("Staging - Début")
    staging = Staging(dsn_file, urlMongo, portMongo, base_dsa)
    staging.setStaging()
    print("Staging - Fin")

    print("Relational - Début")
    relational = Relational(urlMongo, portMongo, base_dsa)
    relational.setRelational(staging.getStaging())
    print("Relational - Fin")

    print("Entity - Début")
    entity = Entity(urlMongo, portMongo, base_dsa, base_dpa)
    entity.setEntity()
    print("Entity - Fin")

    print("Foreign Keys - Début")
    keys = ForeignTable(urlMongo, portMongo, base_dsa, base_dpa)
    keys.setKeysTable()
    print("Foreign Keys - Fin")
