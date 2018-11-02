## KIRO - Kafka In, Rest Out

This is an example of two topics consumed as kstream & ktable materialised to a third topic from which data is served by a rest endpoint 

The original code was taken from [here](https://github.com/spring-cloud/spring-cloud-stream-samples/tree/master/kafka-streams-samples/kafka-streams-table-join)

The application uses two inputs - one KStream for user-clicks and a KTable for user-regions.
Then it joins the information from stream to table to find out total clicks per region.

### Run against kafka (from wurstmeister/kafka image) on docker using the docker-compose.yml at the root 

`docker-compose up -d`

### Build the project

`gradle build`

### Run

`gradle bootRun`

### Consume the output-topic where the merged data messages are writen to

`docker exec -it item-offer /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic output-topic --key-deserializer org.apache.kafka.common.serialization.StringDeserializer --value-deserializer org.apache.kafka.common.serialization.StringDeserializer --property print.key=true --property key.separator="-"`

### Add some data

Run the stand-alone `Producers` application

### Curl the endpoint

`curl http://localhost:8080/counts`
