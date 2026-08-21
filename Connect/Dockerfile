FROM confluentinc/cp-kafka-connect:7.9.2

USER root

RUN confluent-hub install --no-prompt \
    confluentinc/kafka-connect-hdfs3:3.0.12

USER appuser
