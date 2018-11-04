package acme.media.kafkainrestout;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.streams.kstream.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.messaging.handler.annotation.SendTo;

import java.io.IOException;


@SpringBootApplication
public class KafkaInRestOutApplication {

    private static final ObjectMapper mapper = new ObjectMapper();


    //private final MongoTemplate mongoTemplate;

    public static void main(String[] args) {
        SpringApplication.run(KafkaInRestOutApplication.class, args);
    }

    @EnableBinding(KStreamProcessorX.class)
    public static class KStreamToTableJoinApplication {

        public final static String OFFERED_ITEM_ID = "offeredItemId";

        private static String lookupOfferedItemId(String key, String value) {
            return getKeyFromJson(value, OFFERED_ITEM_ID);
        }

        private static String lookupItemId(String key, String value) {
            return getKeyFromJson(value, "itemId");
        }

        @StreamListener
        @SendTo("item-offer")
        public @Output("item-offer")
        KStream<String, String> process(
                @Input("offered-item") KTable<String, String> offeredItem,
                @Input("offer") KTable<String, String> offer,
                @Input("item") KTable<String, String> item) {

            return offer.toStream()
                    .selectKey(KStreamToTableJoinApplication::lookupOfferedItemId)
                    .peek((key, value) -> System.out.println("[1 - re-keyed by offeredItemId] key: " + key + " val: " + value + "\n"))
                    .join(offeredItem, KafkaInRestOutApplication::combine)
                    .peek((key, value) -> System.out.println("[2 - offers combined] key: " + key + " val: " + value + "\n"))
                    .selectKey(KStreamToTableJoinApplication::lookupItemId)
                    .peek((key, value) -> System.out.println("[3 - re-keyed by itemId] key: " + key + " val: " + value + "\n"))
                    .groupByKey()
                    .reduce(String::concat)
                    .toStream()
                    .peek((key, value) -> System.out.println("[4 - group by itemId] key: " + key + " val: " + value + "\n"));



            /*

            >>>wip<<<
            offer.toStream()
                    .selectKey((key, value) -> getKeyFromJson(value, "offeredItemId"))
                    //.peek((key, value) -> System.out.println("(1) This is the key: " + key + " and this is the value: " + value + "\n"))
                    .join(offeredItem, KafkaInRestOutApplication::combine)
                    .peek((key, value) -> System.out.println("(2) This is the key: " + key + " and this is the value: " + value + "\n"))
                    .selectKey((key, value) -> getKeyFromJson(value, "itemId"))
                    .groupByKey()
                    .aggregate(String::new, (key, value, aggregate) -> {
                        aggregate.concat(value).concat(",");
                        return aggregate;
                    })
                    .toStream()
                    .peek((key, value) -> System.out.println("(3) This is the key: " + key + " and this is the value: " + value + "\n"));




             > > > keep this - joining the re-keyed offers to the offeredItem table and concatinating the values

            return offer.toStream()
                    .selectKey((key, value) -> getKeyFromJson(value, "offeredItemId"))
                    .peek((key, value) -> System.out.println("This is the key: " + key
                            + " and this is the value: " + value + "\n"))
                    .join(offeredItem, (value1, value2) -> value1+value2)
                    .peek((key, value) -> System.out.println("This is the key: " + key
                            + " and this is the value: " + value + "\n"));

             > > > keep this - rekeying the offer by offeredItemId for joining to offeredItem link table < < <


            return offer.toStream()
                    .selectKey((key, value) -> getKeyFromJson(value, "offeredItemId"))
                    .peek((key, value) -> System.out.println("This is the key: " + key
                    + " and this is the value: " + value + "\n"));
            */


             /*
            KTable<String, String> offeredItemList = offerItem.toStream()
                    .groupByKey()
                    .aggregate(String::new,
                            (key, value, aggregate) -> {
                        aggregate.concat(value).concat("added");
                        return aggregate;
                            },
                            Materialized.with(
                                    new org.apache.kafka.common.serialization.Serdes.StringSerde(),
                                    new org.apache.kafka.common.serialization.Serdes.StringSerde()));








            KTable<String, String> result =
                    offerTable
                            .groupBy((key, value) -> getKeyFromJson(value, "offeredItemId"))
                            .aggregate(String::new, (key, value, aggregate) -> aggregate.concat(value));


            KTable<String, String> stringStringKTable = result.leftJoin(offerItemTable, (value1, value2) -> value1 + value2);


            //KTable<String, String> join = result.join(offerTable, (value1, value2) -> value1 + value2);


                   /* offerTable.join(offerItemTable, (value1, value2) -> value1 + value2)
                    .leftJoin(offerTable, (value1, value2) -> value1 + value2)
                    ;*/


            /*KTable<String, String> collated =
                    itemTable.leftJoin(offerItemTable, (leftValue, rightValue) ->
                            rightValue);

            KTable<String, String> join = collated.join(offerTable, (value1, value2) -> "left=" + value1 + ", right=" + value2);*/

            //return offeredItemList.toStream();//.peek((key, value) -> System.out.println("key: "+ key + " value: " + value));



        }
    }

    private static String combine(String value1, String value2){

        JsonNode offerWithItemId = null;

        try {

            // get item id from the link table json
            JsonNode offeredItemItemId = mapper.readTree(value2);
            String itemId = offeredItemItemId.get("itemId").asText();

            // add the item id to the offer json
            offerWithItemId = mapper.readTree(value1);
            ((ObjectNode)offerWithItemId).put("itemId", itemId);

        } catch (IOException e) {
            e.printStackTrace();
        }
        return offerWithItemId.toString();
    }

    private static String getKeyFromJson(final String jsonString, final String property) {
        JsonNode itemAsJsonNode = null;
        try {
            itemAsJsonNode = mapper.readTree(jsonString);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return itemAsJsonNode.get(property).asText();
    }

    interface KStreamProcessorX  {

        @Input("item")
        KTable<?, ?> item();

        @Input("offer")
        KTable<?, ?> offer();

        @Input("offered-item")
        KTable<?, ?> offeredItem();

        @Output("item-offer")
        KStream<?,?> output();
    }



}
