# Start zookeeper server

# If you do not have Zookeeper, use: bin/zookeeper-server-start.sh config/zookeeper.properties
$ZOOKEEPER_HOME/bin/zkServer start

# Start kafka server
cd $KAFKA_HOME
bin/kafka-servber-start.sh config/server.properties