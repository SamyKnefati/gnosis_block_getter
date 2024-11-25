from kafka import KafkaConsumer
import requests  # Si tu récupères les transactions via une API HTTP
import psycopg2
import json
from web3 import Web3
from psycopg2.extras import execute_values


w3 = Web3(Web3.HTTPProvider("https://rpc.gnosischain.com"))


def postgre_conn():
    # Connexion à PostgreSQL
    conn = psycopg2.connect(
        dbname="gnosis", user="samy", password="samy", host="localhost", port="5432"
    )
    cursor = conn.cursor()
    return conn, cursor


# Configurer le consommateur Kafka
def get_consumer():
    return KafkaConsumer(
        "gnosis_blocks",
        bootstrap_servers="localhost:9093",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )


def fetch_transactions(transactions_array):
    try:
        transaction_detail = []
        for tx_hash in transactions_array:
            transaction_detail.append(w3.eth.get_transaction(tx_hash))
    except KeyError as e:
        print(f"Clé manquante dans la transaction : {e}")
    return transaction_detail


from psycopg2.extras import execute_values

def insert_transactions(transactions, cursor, conn):
    
    # Préparation des données pour l'insertion en batch
    values = []
    for tx in transactions:
        try:
            values.append((
                tx["blockHash"].hex() if tx.get("blockHash") else 0,
                tx["blockNumber"] if tx.get("blockNumber") else None,
                tx["from"] if tx.get("from") else None,
                tx["gas"] if tx.get("gas") else None,
                tx["gasPrice"] if tx.get("gasPrice") else None,
                tx["hash"].hex() if tx.get("hash") else None,
                tx["input"].hex() if tx.get("input") else None,
                tx["nonce"] if tx.get("nonce") else 0,
                tx.get("to"),  # Peut être NULL pour certaines transactions
                tx["transactionIndex"],
                tx["value"] if tx.get("value") else 0,
                tx.get("type"),
                tx["chainId"] if tx.get("chainId") else 1,
                tx["v"],
                tx["r"].hex() if tx.get("r") else None,
                tx["s"].hex() if tx.get("s") else None
            ))
        except KeyError as e:
            print(f"Clé manquante dans la transaction : {e}")
            continue  # Ignorer cette transaction et continuer

    # Vérifier s'il y a des valeurs à insérer
    if not values:
        print("Aucune transaction valide à insérer.")
        return

    # Requête SQL mise à jour
    query = """
        INSERT INTO transactions (
            block_hash, block_number, from_address, gas, gas_price,
            tx_hash, input_data, nonce, to_address, transaction_index,
            value, type, chain_id, v, r, s
        )
        VALUES %s
        ON CONFLICT (tx_hash) DO NOTHING
    """
    try:
        execute_values(cursor, query, values)
        conn.commit()
        print(f"{len(values)} transactions insérées avec succès.")
    except Exception as e:
        print(f"Erreur lors de l'insertion : {e}")
        conn.rollback()





if __name__ == "__main__":
    consumer = get_consumer()
    conn, cursor = postgre_conn()
    try:
        for message in consumer:
            block = message.value
            transactions_array = block["transactions"]
            block_number =block['block_number']
            print(f"Traitement du bloc {block['block_number']}")
            

            # Récupérer les transactions associées
            transactions = fetch_transactions(transactions_array)

            # Insérer les transactions dans PostgreSQL
            if transactions:
                insert_transactions(transactions)
            else:
                print(f"Aucune transaction trouvée pour le bloc {block_number}.")
    except KeyboardInterrupt:
        print("Arrêté par l'utilisateur.")
    finally:
        consumer.close()
        cursor.close()
        conn.close()
