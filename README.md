# Kafka

<b> Setup: </b>

Confluent Kafka

Databricks Community edition



<b> Steps: </b>
1) Create a cluster and topic_0 in confluent kafka
2) Setup a new client. 
3) Download configuration snippet for your clients (client.conf file)
4) Setup Databricks and create a cluster
5) copy client.conf file to location "dbfs:/FileStore/client.conf"
6) Setup appropriate environment varaible to be used in the notebooks
7) Import kafka-confluent-producer.py and kafka-iot-streaming-consumer.py into your databricks workspace



<b> Python Files: </b>
1) kafka-confluent-producer.py - produces temperature data into kafka topic
2) kafka-iot-streaming-consumer.py - consumes temperature and performs basic transformations and load into SQL Server table
