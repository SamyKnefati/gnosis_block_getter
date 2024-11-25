import psycopg2
from kafka import KafkaProducer
import json
import time

from dotenv import load_dotenv
import os

# Charger les variables d'environnement
load_dotenv()


def postgre_conn():
    # Connexion à PostgreSQL
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),  # Adresse de l'hôte
        database=os.getenv("DB_NAME"),  # Nom de la base de données
        user=os.getenv("DB_USER"),  # Nom d'utilisateur
        password=os.getenv("DB_PASSWORD"),
    )  # Mot de passe
    cursor = conn.cursor()
    return conn, cursor


conn, cursor = postgre_conn()

# Configurer le producteur Kafka
producer = KafkaProducer(
    bootstrap_servers=os.getenv(
        "KAFKA_HOST"
    ),  # Remplace par l'adresse de ton broker Kafka
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


# Décoder les transactions pour obtenir les IDs
def extract_transaction_ids(transactions):
    return ["0x" + tx_bytea.tobytes().hex() for tx_bytea in transactions]


# Fonction pour récupérer les nouveaux blocs
def fetch_and_send_blocks():
    # Dernier numéro de bloc traité, initialise-le à partir d'un état précédent si disponible
    cursor.execute("SELECT MAX(number) AS highest_block FROM blocks;")
    highest_number = cursor.fetchall()

    if highest_number:
        last_block_number = highest_number[0][0] - 10
    else:
        highest_number = 37177311
    while True:
        # Requête pour obtenir les blocs ajoutés après le dernier bloc traité

        cursor.execute(
            """
            SELECT number, timestamp, transactions
            FROM blocks
            WHERE number > %s
            ORDER BY number ASC
        """,
            (last_block_number,),
        )

        rows = cursor.fetchall()

        for row in rows:
            if row[2] is not None:
                block_data = {
                    "block_number": row[0],
                    "timestamp": row[1],
                    "transactions": extract_transaction_ids(row[2]),
                }

            else:
                block_data = {
                    "block_number": row[0],
                    "timestamp": row[1],
                    "transactions": [],
                }
            # Envoyer à Kafka
            producer.send("gnosis_blocks", block_data)
            print(f"Bloc {block_data['block_number']} envoyé à Kafka.")

            # Mettre à jour le dernier bloc traité
            last_block_number = block_data["block_number"]
        # Pause pour éviter une surcharge
        time.sleep(5)


if __name__ == "__main__":
    try:
        fetch_and_send_blocks()
    except KeyboardInterrupt:
        print("Arrêté par l'utilisateur.")
    finally:
        cursor.close()
        conn.close()
