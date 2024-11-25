CREATE TABLE transactions (
    block_hash BYTEA NOT NULL,                   -- HexBytes pour le hash du bloc
    block_number BIGINT NOT NULL,               -- Numéro du bloc (entier)
    from_address VARCHAR(42) NOT NULL,          -- Adresse Ethereum de l'émetteur
    gas BIGINT NOT NULL,                        -- Quantité de gas
    gas_price BIGINT NOT NULL,                  -- Prix du gas
    tx_hash BYTEA NOT NULL PRIMARY KEY,         -- HexBytes pour le hash de la transaction (clé primaire)
    input_data BYTEA,                           -- HexBytes pour les données d'entrée
    nonce BIGINT NOT NULL,                      -- Nonce de la transaction
    to_address VARCHAR(42),                     -- Adresse Ethereum du destinataire (peut être NULL)
    transaction_index INT NOT NULL,             -- Index de la transaction dans le bloc
    value NUMERIC(78, 0) NOT NULL,              -- Valeur de la transaction (utilisation de NUMERIC pour de grandes valeurs)
    type INT NOT NULL,                          -- Type de la transaction
    chain_id INT NOT NULL,                      -- ID de la chaîne
    v INT NOT NULL,                             -- Champ "v" de la signature
    r BYTEA NOT NULL,                           -- HexBytes pour le champ "r" de la signature
    s BYTEA NOT NULL                            -- HexBytes pour le champ "s" de la signature
);
