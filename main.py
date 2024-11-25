from web3 import Web3
import asyncio
from queue import Queue
from database.database_ingestion import insert_into_db
from kafka_process.block_producer import fetch_and_send_blocks
from kafka_process.block_consumer import (
    get_consumer,
    fetch_transactions,
    insert_transactions,
    postgre_conn,
)
import threading
import asyncio
from database.database_ingestion import main_getter 


# Thread pour le producer
def producer_thread(conn, cursor):
    try:
        fetch_and_send_blocks()
    except KeyboardInterrupt:
        print("Producer arrêté par l'utilisateur.")
    finally:
        # Fermer les connexions (ajuste ces variables selon ton script)
        cursor.close()
        conn.close()


# Thread pour le consumer
def consumer_thread():
    consumer = get_consumer()
    conn, cursor = postgre_conn()

    try:
        for message in consumer:
            block = message.value
            block_number = block["block_number"]
            print(f"Traitement du bloc {block_number}")

            # Récupérer les transactions associées
            transactions = fetch_transactions(block["transactions"])

            # Insérer les transactions dans PostgreSQL
            if transactions:
                insert_transactions(transactions, cursor=cursor, conn= conn)
                print(
                    f"{len(transactions)} transactions insérées pour le bloc {block_number}."
                )
            else:
                print(f"Aucune transaction trouvée pour le bloc {block_number}.")
    except KeyboardInterrupt:
        print("Arrêté par l'utilisateur.")
    finally:
        consumer.close()
        cursor.close()
        conn.close()


# Thread pour l'exécution asynchrone du getter
def getter_thread():
    try:
        main_getter()
    except KeyboardInterrupt:
        print("Getter arrêté par l'utilisateur.")


def main():
    conn, cursor = postgre_conn()

    # Création des threads
    threads = [
        threading.Thread(target=getter_thread),
        threading.Thread(target=producer_thread, args=(conn, cursor)),
        threading.Thread(target=consumer_thread),
        
    ]

    # Lancer les threads
    for thread in threads:
        thread.start()

    # Attendre que tous les threads se terminent
    for thread in threads:
        thread.join()


if __name__ == "__main__":
    main()
