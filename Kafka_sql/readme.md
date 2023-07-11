Vehicle Toll Plaza - Real-Time Data Processing

This project contains two Python scripts that together simulate the collection and processing of real-time data from a vehicle toll plaza. The scripts make use of Apache Kafka for streaming data and MySQL for data storage.
traffic_simulator.py

This script simulates vehicles passing through a toll plaza. It randomly generates data about each vehicle, including the time of passage, the vehicle ID, the type of vehicle, and the ID of the toll plaza. This data is then sent to a Kafka topic named 'toll'.

To run this script, make sure you have Kafka running and accessible at localhost:9092.
streaming_consumer.py

This script consumes the data from the 'toll' Kafka topic and stores it into a MySQL database. It connects to a MySQL database using the provided credentials, transforms the timestamp format to match the database schema, and inserts the data into a table named 'livetolldata'.

To run this script, make sure you have a MySQL server running and accessible with the provided credentials. Also, the 'toll' Kafka topic needs to have data being produced to it, which can be done by running traffic_simulator.py.
Dependencies

To run these scripts, you need the following Python packages:

    kafka-python: To connect to and interact with Kafka.
    mysql-connector-python: To connect to and interact with MySQL.

You can install them using pip:

pip install kafka-python mysql-connector-python

Running the Scripts

    Start your Kafka and MySQL servers.

    Run the traffic_simulator.py script to start producing data to the Kafka topic:

python traffic_simulator.py

    In a separate terminal, run the streaming_consumer.py script to consume the data and store it in the MySQL database:

python streaming_consumer.py

Please note that this is a basic simulation and doesn't handle many edge cases or potential errors that could occur in a production environment. It's intended to be a simple example of how to use Kafka and MySQL together in a Python script.
