# DSN_Normalize
Normalisation des informations stockées dans la DSN (Déclaration Sociale Nominative)

## Arguments / Paramètres
### -c, --chemin
    Obligatoire. Chemin vers le fichier DSN à charger
### -u, --url_mongo
    Obligatoire. Adresse du serveur MongoDB
### -p, --port_mongo
    Obligatoire. Port du serveur mongo
### -s, --dsa_base
    Obligatoire. Base DSA MongoDB
### -t, --dpa_base
    Obligatoire. Base DPA MongoDB
### -m, --dmt_base
    Obligatoire. Base DMT MongoDB

#### Le shell startMain.sh contient la commande suivante :
    python3 main.py -c "Files/D3N_201701.txt" \
                    -u "172.17.0.1" \
                    -p 27017 \
                    -s "dsn_dsa" \
                    -t "dsn_dpa" \
                    -m "dsn_dmt"
    Il convient de l'ajuster en fonction de vos besoins.

## Base de données
    La base de données utilisée est MongoDB

## Phases de traitement
    Le traitement est découpé en trois grandes phases :
        - DSA - Data Staging Area - Staging
        - DPA - Data Processing Area - Transformation de la donnée
        - DMT - Datamart - Etage final de consommation de la donnée

### DSA - Data Staging Area
#### DSA - Référentiels rubriques DSN
    Referentiel.py
    Chargement du plan de rubriques DSN ainsi que des liens avec la structure mère

#### DSA - Staging - Traitement 1
    Staging.py
    Chargement du fichier DSN dans la collection dsa_staging.

#### DSA - Relational - Traitement 2
    Relational.py
    Calcul des clés par bloc et préparation du regroupement des informations par 
    entités de la DSN. La collection alimentée est dsa_relational

### DPA - Data Processing Area
#### DPA - Entity - Traitement 1
    Entity.py
    Génération des collections par entités de la DSN. Chaque entité est reliée à sa structure
    mère via clé étrangère (FK).

#### DPA - ForeignTable - Traitement 2
    ForeignTable.py
    Génération des collections permettant le lien entre chaque entité et ses structures mères.
