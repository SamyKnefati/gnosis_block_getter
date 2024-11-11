from web3 import Web3
import asyncio
import queue


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


# Fonction pour traiter les transactions
async def process_transactions():
    while True:
        # Attendre un bloc de manière asynchrone sans bloquer l'exécution
        block = await block_queue.get()  # Attente asynchrone pour récupérer un bloc
        print(f"Traitement du bloc : {block['number']}")

        # Ajouter les transactions du bloc à la queue des transactions
        for tx_hash in block["transactions"]:
            await tx_queue.put(tx_hash)  # Utilisation asynchrone


# Fonction pour surveiller et afficher la taille des queues
async def monitor_queues():
    while True:
        # Afficher la taille de la queue des blocs et des transactions
        print(
            f"Taille de block_queue : {block_queue.qsize()} | Taille de tx_queue : {tx_queue.qsize()}"
        )
        await asyncio.sleep(2)  # Attendre 2 secondes avant de réafficher


async def main():
    await asyncio.gather(
        listen_for_blocks(),
        process_transactions(),
        monitor_queues(),
    )


# Exécuter l'event loop principal
if __name__ == "__main__":
    asyncio.run(main())


# # Fonction pour récupérer les détails des transactions
# async def get_transaction_details():
#     while True:
#         tx_hash = await tx_queue.get()  # Attente asynchrone pour les transactions
#         try:
#             tx = web3.eth.get_transaction(tx_hash)
#             print(f"Transaction : {tx_hash}")
#             print(f"From : {tx['from']}")
#             print(f"To : {tx['to']}")
#             print(f"Value : {web3.from_wei(tx['value'], 'ether')} ETH")
#             print("----------------------------------------")
#         except Exception as e:
#             print(f"Error retrieving transaction details for {tx_hash}: {e}")


# async def main():
#     # Créer les tâches
#     block_task = asyncio.create_task(listen_for_blocks())
#     tx_task = asyncio.create_task(process_transactions())
#     details_task = asyncio.create_task(get_transaction_details())

#     # Attendre que toutes les tâches se terminent
#     await asyncio.gather(block_task, tx_task, details_task)

#     # Attendre que la tâche de listen_for_blocks se termine
#     await block_task


# # Exécuter la fonction principale
# asyncio.run(main())
