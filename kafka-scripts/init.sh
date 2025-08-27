bash /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker:9092 --create --topic in-gps --config retention.ms=1800000
bash /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker:9092 --create --topic out-nearby-person --config retention.ms=1800000
