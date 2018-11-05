package acme.media.kafkainrestout;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.messaging.handler.annotation.SendTo;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Objects;


@SpringBootApplication
public class KafkaInRestOutApplication {

    private static final ObjectMapper mapper = new ObjectMapper();


    //private final MongoTemplate mongoTemplate;

    public static void main(String[] args) {
        SpringApplication.run(KafkaInRestOutApplication.class, args);
    }

    @EnableBinding(KStreamProcessorX.class)
    public static class KStreamToTableJoinApplication {

        @StreamListener
        @SendTo("item-offer")
        public @Output("item-offer")
        KStream<String, String> process(
                @Input("offered-item") KTable<String, String> offeredItem,
                @Input("offer") KStream<String, String> offer,
                @Input("item") KTable<String, String> item) {


            KStream<String, String> offersByItemId = offer
                    .selectKey((key, value) -> getKeyFromJson(value, "offeredItemId"))
                    .peek((key, value) -> inspect(key, value, "[1]"))
                    .join(offeredItem, KafkaInRestOutApplication::combine)
                    .peek((key, value) -> inspect(key, value, "[2] key: "))
                    .selectKey((key, value) -> getKeyFromJson(value, "itemId"))
                    .peek((key, value) -> inspect(key, value, "[3] key: "))
                    .groupByKey()
                    .reduce(String::concat)
                    .toStream().peek((key, value) -> inspect(key, value, "([4] key: "));


                    //.toStream()
                    //.peek((key, value) -> System.out.println("([4] key: " + key + " val: " + value + "\n"));

            return offersByItemId;//item.leftJoin(offersByItemId, String::concat).toStream().peek((key, value) -> System.out.println("([4] key: " + key + " val: " + value + "\n"));


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
            throw new UncheckedIOException(e);
        }
        return offerWithItemId.toString();
    }

    private static String getKeyFromJson(final String jsonString, final String property) {
        JsonNode itemAsJsonNode = null;
        try {
            itemAsJsonNode = mapper.readTree(jsonString);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return itemAsJsonNode.get(property).asText();
    }

    interface KStreamProcessorX  {

        @Input("item")
        KTable<?, ?> item();

        @Input("offer")
        KStream<?, ?> offer();

        @Input("offered-item")
        KTable<?, ?> offeredItem();

        @Output("item-offer")
        KStream<?,?> output();

    }

    private static void inspect(String key, String value, String location) {
        System.out.format("%s key: %s val: %s%n", location, key, value);
    }
}
