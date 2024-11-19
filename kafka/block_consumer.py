from kafka import KafkaConsumer
import requests  # Si tu récupères les transactions via une API HTTP
import psycopg2
import json

# Connexion à PostgreSQL
conn = psycopg2.connect(
    dbname="gnosis",
    user="samy",
    password="samy",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

# Configurer le consommateur Kafka
consumer = KafkaConsumer(
    'gnosis_blocks',
    bootstrap_servers='localhost:9093',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def fetch_transactions(block_number):
    # Remplace l'URL et les paramètres selon ton API
    url = f"https://api.gnosis-chain.io/block/{block_number}/transactions"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()  # Assure-toi que tu récupères les données nécessaires
    else:
        print(f"Erreur lors de la récupération des transactions pour le bloc {block_number}.")
        return []

def insert_transactions(transactions):
    for tx in transactions:
        # Ajuste les champs en fonction des données de la transaction que tu récupères
        cursor.execute("""
            INSERT INTO transactions (tx_hash, block_number, from_address, to_address, value, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (tx_hash) DO NOTHING
        """, (
            tx['hash'],
            tx['block_number'],
            tx['from'],
            tx['to'],
            tx['value'],
            tx['timestamp']
        ))
        conn.commit()

if __name__ == "__main__":
    try:
        for message in consumer:
            block = message.value
            block_number = block['block_number']
            print(f"Traitement du bloc {block_number}")
            
            # Récupérer les transactions associées
            transactions = fetch_transactions(block_number)
            
            # Insérer les transactions dans PostgreSQL
            if transactions:
                insert_transactions(transactions)
                print(f"{len(transactions)} transactions insérées pour le bloc {block_number}.")
            else:
                print(f"Aucune transaction trouvée pour le bloc {block_number}.")
    except KeyboardInterrupt:
        print("Arrêté par l'utilisateur.")
    finally:
        consumer.close()
        cursor.close()
        conn.close()
