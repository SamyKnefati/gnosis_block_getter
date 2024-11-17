import json
from web3 import Web3
import asyncio
import asyncpg
from datetime import datetime

# Configuration de la connexion WebSocket
ws_url = "wss://rpc.gnosischain.com/wss"
web3 = Web3(Web3.LegacyWebSocketProvider(ws_url))


# Fonction asynchrone pour insérer les blocs dans PostgreSQL
async def insert_into_db(block):
    try:

        conn = await asyncpg.connect(
            host="localhost",  # Remplace par l'adresse de ton hôte
            database="gnosis",  # Remplace par le nom de ta base de données
            user="samy",  # Remplace par ton nom d'utilisateur PostgreSQL
            password="samy",  # Remplace par ton mot de passe
        )

        await conn.execute(
            """
        INSERT INTO blocks (author, difficulty, 
        extra_data, gas_limit, gas_used, hash, logs_bloom, miner, mix_hash, 
        nonce, number,
         parent_hash,
         receipts_root,
         sha3_uncles,
         size,
         state_root,
         total_difficulty,
         timestamp,
         base_fee_per_gas,
         transactions,
         transactions_root,
         uncles,
         withdrawals,
         withdrawals_root,
         blob_gas_used,
         excess_blob_gas,
         parent_beacon_block_root
         )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, 
        $19, $20, $21, $22, $23,$24,$25,$26,$27)
        """,
            block.get("author", None),  # Exemple d'auteur
            block.get("difficulty", None),
            bytes(block["extraData"]) if "extraData" in block else None,
            block.get("gasLimit", None),
            block.get("gasUsed", None),
            bytes(block["hash"]) if "hash" in block else None,
            bytes(block["logsBloom"]) if "logsBloom" in block else None,
            block.get("miner", None),
            bytes(block["mixHash"]) if "mixHash" in block else None,
            bytes(block["nonce"]) if "nonce" in block else None,
            block.get("number", None),
            bytes(block["parentHash"]) if "parentHash" in block else None,
            bytes(block["receiptsRoot"]) if "receiptsRoot" in block else None,
            bytes(block["sha3Uncles"]) if "sha3Uncles" in block else None,
            block.get("size", None),
            bytes(block["stateRoot"]) if "stateRoot" in block else None,
            block.get("totalDifficulty", None),
            block.get("timestamp", None),
            block.get("baseFeePerGas", 0),
            (
                [bytes(tx) for tx in block["transactions"]]
                if block.get("transactions")
                else None
            ),
            bytes(block["transactionsRoot"]) if "transactionsRoot" in block else None,
            (
                [bytes(uncle) for uncle in block["uncles"]]
                if block.get("uncles")
                else None
            ),
            (
                [str(wd).encode("utf-8") for wd in block["withdrawals"]]
                if "withdrawals" in block
                else None
            ),
            
            bytes(block["withdrawalsRoot"]) if "withdrawalsRoot" in block else None,
            block.get("blobGasUsed", 0),
            block.get("excessBlobGas", 0),
            (
                bytes(block["parentBeaconBlockRoot"])
                if "parentBeaconBlockRoot" in block
                else None
            ),
        )
        # await conn.execute(
        #     """
        #     INSERT INTO blocks (
        #         author,
        #         difficulty,
        #         extra_data,
        #         gas_limit,
        #         gas_used,
        #         hash,
        #         logs_bloom,
        #         miner,
        #         mix_hash,
        #         nonce,
        #         number,
        #         parent_hash,
        #         receipts_root,
        #         sha3_uncles,
        #         size,
        #         state_root,
        #         total_difficulty,
        #         timestamp,
        #         base_fee_per_gas,
        #         transactions,
        #         transactions_root,
        #         uncles,
        #         withdrawals,
        #         withdrawals_root,
        #         blob_gas_used,
        #         excess_blob_gas,
        #         parent_beacon_block_root
        #     ) VALUES (
        #         $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
        #         $11, $12, $13, $14, $15, $16, $17, $18,
        #         $19, $20, $21, $22, $23, $24, $25, $26
        #     )
        # """,
        #     block.get("author", None),
        #     block.get("difficulty", None),
        #     bytes(block["extraData"]) if "extraData" in block else None,
        #     block.get("gasLimit", None),
        #     block.get("gasUsed", None),
        #     bytes(block["hash"]) if "hash" in block else None,
        #     bytes(block["logsBloom"]) if "logsBloom" in block else None,
        #     block.get("miner", None),
        #     bytes(block["mixHash"]) if "mixHash" in block else None,
        #     bytes(block["nonce"]) if "nonce" in block else None,
        #     block.get("number", None),
        #     bytes(block["parentHash"]) if "parentHash" in block else None,
        #     bytes(block["receiptsRoot"]) if "receiptsRoot" in block else None,
        #     bytes(block["sha3Uncles"]) if "sha3Uncles" in block else None,
        #     block.get("size", None),
        #     bytes(block["stateRoot"]) if "stateRoot" in block else None,
        #     block.get("totalDifficulty", None),
        #     block.get("timestamp", None),
        #     block.get("baseFeePerGas", 0),
        #     (
        #         [bytes(tx) for tx in block["transactions"]]
        #         if block.get("transactions")
        #         else None
        #     ),
        #     bytes(block["transactionsRoot"]) if "transactionsRoot" in block else None,
        #     (
        #         [bytes(uncle) for uncle in block["uncles"]]
        #         if block.get("uncles")
        #         else None
        #     ),
        #     (
        #         [bytes(wd) for wd in block["withdrawals"]]
        #         if "withdrawals" in block
        #         else None
        #     ),
        #     bytes(block["withdrawalsRoot"]) if "withdrawalsRoot" in block else None,
        #     block.get("blobGasUsed", 0),
        #     block.get("excessBlobGas", 0),
        #     (
        #         bytes(block["parentBeaconBlockRoot"])
        #         if "parentBeaconBlockRoot" in block
        #         else None
        #     ),
        # )
        # await conn.execute(
        #     """
        #     INSERT INTO blocks (
        #         author,

        #     ) VALUES (
        #         $1
        #     )
        # """,
        #     block.get("author", None),
        #     block.get("difficulty", None),
        #     bytes(block["extraData"]) if "extraData" in block else None,
        #     block.get("gasLimit", None),
        #     block.get("gasUsed", None),
        #     bytes(block["hash"]) if "hash" in block else None,
        #     bytes(block["logsBloom"]) if "logsBloom" in block else None,
        #     block.get("miner", None),
        #     bytes(block["mixHash"]) if "mixHash" in block else None,
        #     bytes(block["nonce"]) if "nonce" in block else None,
        #     block.get("number", None),
        #     bytes(block["parentHash"]) if "parentHash" in block else None,
        #     bytes(block["receiptsRoot"]) if "receiptsRoot" in block else None,
        #     bytes(block["sha3Uncles"]) if "sha3Uncles" in block else None,
        #     block.get("size", None),
        #     bytes(block["stateRoot"]) if "stateRoot" in block else None,
        #     block.get("totalDifficulty", None),
        #     block.get("timestamp", None),
        #     block.get("baseFeePerGas", 0),
        #     (
        #         [bytes(tx) for tx in block["transactions"]]
        #         if block.get("transactions")
        #         else None
        #     ),
        #     bytes(block["transactionsRoot"]) if "transactionsRoot" in block else None,
        #     (
        #         [bytes(uncle) for uncle in block["uncles"]]
        #         if block.get("uncles")
        #         else None
        #     ),
        #     (
        #         [bytes(wd) for wd in block["withdrawals"]]
        #         if "withdrawals" in block
        #         else None
        #     ),
        #     bytes(block["withdrawalsRoot"]) if "withdrawalsRoot" in block else None,
        #     block.get("blobGasUsed", 0),
        #     block.get("excessBlobGas", 0),
        #     (
        #         bytes(block["parentBeaconBlockRoot"])
        #         if "parentBeaconBlockRoot" in block
        #         else None
        #     ),
        # )

        await conn.close()
    except Exception as e:
        print(f"Erreur lors de l'insertion en base de données : {e}")
