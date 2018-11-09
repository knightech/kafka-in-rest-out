package acme.media.kafkainrestout;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.streams.kstream.*;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.messaging.handler.annotation.SendTo;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * This class manages re-keying and aggregating offers by itemId
 */
@SpringBootApplication
@Profile("oa")
public class OfferAggregator {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) {
        SpringApplication.run(OfferAggregator.class, args);
    }

    @Bean
    CommandLineRunner runner(){
        return args  -> {
            new OfferAggregatorProcessor();
        };
    }

    @EnableBinding(OfferBinding.class)
    public class OfferAggregatorProcessor {

        @StreamListener
        @SendTo("aggregated-offers-out")
        public KStream<String, String> process(

                @Input("offered-items") KTable<String, String> offeredItems,

                @Input("offers") KStream<String, String> offers

                ) {

            return offers.selectKey((key, value) -> getKeyFromJson(value, "offeredItemId"))

                    .through("offers-by-offered-itemId")

                    .peek((key, value) -> printForDebug(key, value, 1))

                    .join(offeredItems, OfferAggregator::combine)

                    .peek((key, value) -> printForDebug(key, value, 2))

                    .selectKey((key, value) -> getKeyFromJson(value, "itemId"))

                    .through("offers-by-itemId")

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

                    .peek((key, value) -> printForDebug(key, value, 6));

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

    interface OfferBinding {

        @Input("offered-items")
        KTable<?, ?> offeredItem();

        @Input("offers")
        KStream<?, ?> offer();

        @Output("aggregated-offers-out")
        KStream<?, ?> aggregatedOffersOut();

    }

    private static void printForDebug(String key, String value, int location) {
        System.out.format("[%d] key: %s val: %s%n", location, key, value);
    }
}
