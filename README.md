# Gnosis Block Getter

## Overview
**Gnosis Block Getter** is a Python project designed to fetch and store Gnosis Chain blocks in real-time using WebSockets. It integrates with PostgreSQL to store transaction data and utilizes Kafka for real-time data processing.

## Features
- Real-time block fetching via WebSockets.
- Storage of transaction data in PostgreSQL.
- Real-time data processing with Kafka.
- Flexible and scalable architecture.

## Prerequisites
Before you start, ensure you have the following installed:
- Python 3.12 
- PostgreSQL
- Docker and Docker Compose
- Kafka
- Web3.py library
- psycopg2 library

## Installation
1. **Clone the repository**:
   ```sh
   git clone https://github.com/SamyKnefati/gnosis_block_getter.git
   cd gnosis_block_getter
2. **Setup a Python environment**
**Create a virtual environment**:
   ```bash
   python3 -m venv venv
   ```
   Activate the virtual environment:

   On Linux/MacOS:
      ```bash
   source venv/bin/activate
      ```

   On Windows:

   ```bash
   venv\Scripts\activate
   ```



3. **Install the required Python packages:**
 ```bash
 pip install -r requirements.txt
```
4. **Set up PostgreSQL:**
Create a database named gnosis_blocks.
Create the necessary tables using the provided SQL script.

5. **Set up Kafka using Docker Compose:**
Start the services using Docker Compose:
```bash
docker-compose up -d
```
