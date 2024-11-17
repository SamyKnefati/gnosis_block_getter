from web3 import Web3
import asyncio
from queue import Queue
from database.database_ingestion import insert_into_db


ws_url = "wss://rpc.gnosischain.com/wss"
web3 = Web3(Web3.LegacyWebSocketProvider(ws_url))

if web3.is_connected():
    print("Connecté avec succès à la Gnosis Chain via WebSocket")
else:
    print("Impossible de se connecter au nœud Gnosis Chain")
    exit()

# Utilisation d'une queue asynchrone
block_queue = asyncio.Queue()
tx_queue = asyncio.Queue()

async def listen_for_blocks():
    try:
        # Crée un filtre pour écouter les nouveaux blocs
        block_filter = web3.eth.filter("latest")
        print("Écoute des nouveaux blocs...")

        while True:
            # Récupère les nouveaux blocs
            for block_hash in block_filter.get_new_entries():
                block = web3.eth.get_block(block_hash)
                # Ajouter le bloc à la queue de manière asynchrone
                await block_queue.put(block)
                print(f"Nouveau bloc reçu : {block['number']}")
                        
                await asyncio.sleep(2)  # Pause pour éviter de surcharger la connexion
    except Exception as e:
        print(f"Erreur lors de l'écoute des blocs : {e}")


# Fonction asynchrone pour traiter l'insertion des blocs
async def process_blocks():
    while True:
        if not block_queue.empty():
            block = await block_queue.get()            
            await insert_into_db(block)
        await asyncio.sleep(1)  # Pause légère pour éviter le "busy-waiting"


# Fonction principale pour exécuter les tâches asynchrones
async def main():
    producer_task = asyncio.create_task(listen_for_blocks())
    consumer_task = asyncio.create_task(process_blocks())
    await asyncio.gather(producer_task, consumer_task)


# Exécuter l'event loop principal
if __name__ == "__main__":
    asyncio.run(main())



