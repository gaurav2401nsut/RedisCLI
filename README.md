Redis-Inspired Key-Value Store
Overview
This project is a Redis-inspired key-value store implemented in Python. It includes a custom server and client system that supports core commands like GET, SET, DELETE, MGET, and MSET using the Redis Serialization Protocol (RESP). The server is designed to handle multiple client connections concurrently with Gevent for scalability and efficiency.

Features
Commands Supported:

GET: Retrieve the value of a key.
SET: Set the value of a key.
DELETE: Remove a key from the store.
FLUSH: Clear all keys from the store.
MGET: Retrieve the values of multiple keys.
MSET: Set multiple keys and values.
Concurrency:

Uses Gevent’s StreamServer and connection pooling for handling multiple client connections efficiently.
Custom RESP Implementation:

Parses and serializes client-server communication for seamless data exchange.
Requirements
Python 3.x
Gevent library
Install Gevent using pip:

bash
Copy code
pip install gevent  
Usage
Running the Server
Start the server by executing the following:

bash
Copy code
python server.py  
Running the Client
Use the client to interact with the server:

python
Copy code
from client import Client  

# Connect to the server  
client = Client(host='127.0.0.1', port=31338)  

# Set a key-value pair  
client.set('key', 'value')  

# Retrieve the value of a key  
value = client.get('key')  
print(value)  # Output: 'value'  

# Delete a key  
client.delete('key')  

# Flush all keys  
client.flush()  
Testing Batch Commands
python
Copy code
# Set multiple keys  
client.mset('key1', 'value1', 'key2', 'value2')  

# Get multiple keys  
values = client.mget('key1', 'key2')  
print(values)  # Output: ['value1', 'value2']   
