## KIRO - Kafka In, Rest Out

This is an example of two topics consumed as kstream & ktable materialised to a third topic from which data is served by a rest endpoint 

The original code was taken from [here](https://github.com/spring-cloud/spring-cloud-stream-samples/tree/master/kafka-streams-samples/kafka-streams-table-join)

The application uses two inputs - one KStream for user-clicks and a KTable for user-regions.
Then it joins the information from stream to table to find out total clicks per region.

### Run against kafka (from wurstmeister/kafka image) on docker using the docker-compose.yml at the root 

`docker-compose up -d`

To force clean set of topics, remove all kafka containers:

`docker-compose stop; docker rm -f $(docker ps -a -q -f name="kiro-zk|kiro-kafka"); docker-compose up -d`
 
Stop and remove ALL (stopped and running) docker containers
`docker rm -f $(docker ps -a -q);` 

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

##### Developer Notes

######Trying to combine offers:
```
offer.toStream()
    .selectKey((key, value) -> getKeyFromJson(value, "offeredItemId"))
    .peek((key, value) -> System.out.println("[1] key: " + key + " val: " + value + "\n"))
    .join(offeredItem, KafkaInRestOutApplication::combine)
    .peek((key, value) -> System.out.println("[2] key: " + key + " val: " + value + "\n"))
    .selectKey((key, value) -> getKeyFromJson(value, "itemId"))
    .peek((key, value) -> System.out.println("[3] key: " + key + " val: " + value + "\n"))
    .groupByKey()
    .reduce(String::concat)
    .toStream()
    .peek((key, value) -> System.out.println("([4] key: " + key + " val: " + value + "\n"));
```

######Joining the re-keyed offers to the offeredItem table and concatinating the values:
```
offer.toStream()
    .selectKey((key, value) -> getKeyFromJson(value, "offeredItemId"))
    .peek((key, value) -> System.out.println("This is the key: " + key
            + " and this is the value: " + value + "\n"))
    .join(offeredItem, (value1, value2) -> value1+value2)
    .peek((key, value) -> System.out.println("This is the key: " + key
            + " and this is the value: " + value + "\n"));
```

######Rekeying the offer by offeredItemId for joining to offeredItem link table:

```
offer.toStream()
    .selectKey((key, value) -> getKeyFromJson(value, "offeredItemId"))
    .peek((key, value) -> System.out.println("This is the key: " + key
    + " and this is the value: " + value + "\n"));
```
