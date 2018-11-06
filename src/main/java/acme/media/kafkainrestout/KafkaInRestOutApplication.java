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
import java.io.UncheckedIOException;


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
        @SendTo("item-with-offers")
        public @Output("item-with-offers")
        KStream<String, String> process(

                @Input("offered-item") KTable<String, String> offeredItem,

                @Input("offer") KStream<String, String> offer,

                @Input("item") KTable<String, String> item

                ) {

            return offer.selectKey((key, value) -> getKeyFromJson(value, "offeredItemId"))

                    .through("offersByOfferedItemId")

                    .peek((key, value) -> printForDebug(key, value, 1))

                    .join(offeredItem, KafkaInRestOutApplication::combine)

                    .peek((key, value) -> printForDebug(key, value, 2))

                    .selectKey((key, value) -> getKeyFromJson(value, "itemId"))

                    .through("offersByItemId")

                    .peek((key, value) -> printForDebug(key, value, 3))

                    .groupByKey()

                    .aggregate(

                            String::new,

                            (key, value, aggregator) -> {

                                printForDebug(key, value, 4);

                                printForDebug(key, aggregator, 5);

                                return aggregator.concat(value);
                            })

                    .toStream()

                    .peek((key, value) -> printForDebug(key, value, 6))

                    .leftJoin(item, (value1, value2) -> value1+value2)

                    .peek((key, value) -> printForDebug(key, value, 7));

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

        @Input("offered-item")
        KTable<?, ?> offeredItem();

        @Input("offer")
        KStream<?, ?> offer();

        @Input("item")
        KTable<?, ?> item();

        @Output("item-with-offers")
        KStream<?,?> itemWithOffers();
    }

    private static void printForDebug(String key, String value, int location) {
        System.out.format("[%d] key: %s val: %s%n", location, key, value);
    }
}
