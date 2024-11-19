CREATE TABLE transactions (
    tx_hash VARCHAR(66) PRIMARY KEY,  -- Longueur typique pour un hash de transaction Ethereum
    block_number INTEGER NOT NULL,
    from_address VARCHAR(42) NOT NULL,  -- Longueur typique pour les adresses Ethereum
    to_address VARCHAR(42),
    value NUMERIC(78, 0),  -- Pour stocker de grands nombres sans virgule décimale (peut être ajusté)
    timestamp TIMESTAMP
);
