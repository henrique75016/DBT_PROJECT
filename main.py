import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.exc import OperationalError

# Paramètres de connexion
username = 'root'
password = 'forzaroma1H!'  # Votre mot de passe
host = 'localhost'
port = '3306'  # Port par défaut MySQL
database_name = 'DBT_PROJECT'  # Remplacez par le nom de votre base de données

# URL de connexion MySQL
DATABASE_URL = f"mysql+pymysql://{username}:{password}@{host}:{port}/{database_name}"

# Création de l'engine SQLAlchemy
engine = create_engine(DATABASE_URL)

# Vérification de l'existence de la base de données
try:
    if database_exists(engine.url):
        print(f"La base de données '{database_name}' existe déjà.")
    else:
        print(f"La base de données '{database_name}' n'existe pas.")
except OperationalError as e:
    print(f"Erreur lors de la connexion à la base de données : {e}")

# Test de la connexion
try:
    with engine.connect() as connection:
        print("Connexion réussie à la base de données.")
except OperationalError as e:
    print(f"Erreur lors de la connexion à la base de données : {e}")

# Création de la base de données si elle n'existe pas
if not database_exists(engine.url):
    create_database(engine.url)
    print(f"La base de données '{database_name}' a été créée.")
else:
    print(f"La base de données '{database_name}' existe déjà.")

# Liste des tables et des fichiers CSV à importer
liste_tables = ["customers", "items", "orders", "products", "stores", "supplies"]

for table in liste_tables:
    csv_url = f"https://raw.githubusercontent.com/dsteddy/jaffle_shop_data/main/raw_{table}.csv"
    try:
        # Téléchargement et lecture du CSV
        df = pd.read_csv(csv_url)
        # Enregistrement du DataFrame dans la base de données MySQL
        df.to_sql(f"raw_{table}", engine, if_exists="replace", index=False)
        print(f"Les données de la table '{table}' ont été importées avec succès.")
    except Exception as e:
        print(f"Erreur lors de l'importation de la table '{table}': {e}")
