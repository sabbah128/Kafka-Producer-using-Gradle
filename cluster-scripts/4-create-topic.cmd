%KAFKA_HOME%\bin\windows\kafka-topics.bat --create --topic data-stream --bootstrap-server localhost:9092 --replication-factor 3 --partitions 3 --config min.insync.replicas=2