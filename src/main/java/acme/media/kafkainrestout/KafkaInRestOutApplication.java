package acme.media.kafkainrestout;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.kafka.streams.QueryableStoreRegistry;
import org.springframework.cloud.stream.binder.kafka.streams.annotations.KafkaStreamsProcessor;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
public class KafkaInRestOutApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaInRestOutApplication.class, args);
    }

    @EnableBinding(KStreamProcessorX.class)
    public static class KStreamToTableJoinApplication {


        @StreamListener
        @SendTo("output")
        public KStream<String, String> process(@Input("item") KStream<String, String> itemStream,
                                             @Input("offer") KTable<String, String> offerTable) {

            // read item json
            // read offer json
            // combine them with the key
            // combined json needs to be stored in using reducer/aggregrator without performing any

            KStream<String, ItemOffer> stringItemOfferKStream = itemStream
                    .leftJoin(offerTable, this::getItemOffer, Joined.with(Serdes.String(), Serdes.String(), null));



            // now put it in store

             return stringItemOfferKStream
                    .map((itemOfferId, itemOffer) -> new KeyValue<>(itemOfferId, itemOffer.getItemOffer()))
                    .groupByKey()
                    .aggregate(
                            String::new,
                            (key, value, aggregate) -> value,
                            Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("item-offer")
                                    .withKeySerde(Serdes.String()).
                                    withValueSerde(Serdes.String())
                    ).toStream();






                    // stream to materialzied view


                  /*  .peek((key, value) -> System.out.println("\nkey " + key + " : " + "value " + value  ))
                    .groupByKey(Serialized.with(Serdes.String(), Serdes.String()))

                    .reduce(String::concat,
                            Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("item-offer")
                            .withKeySerde(Serdes.String()).withValueSerde(Serdes.String()))
                    .toStream().peek((key, value) -> System.out.println("\nkey 2 " + key + " : " + "value 2" + value  ));
*/        }


        private ItemOffer getItemOffer(String item, String offer) {
            return new ItemOffer(offer == null ? "UNKNOWN" : offer, item);
        }


    }


    interface KStreamProcessorX  {


        @Input("item")
        KStream<?, ?> item();

        @Input("output")
        KStream<?, ?> output();

        @Input("offer")
        KTable<?, ?> offer();
    }

    private static final class ItemOffer {

        private final String item;
        private final String offer;

        public ItemOffer(String item, String offer) {

            if (item == null || item.isEmpty()) {
                throw new IllegalArgumentException("item must be set");
            }

            if (offer == null) {
                throw new IllegalArgumentException("offer must be set");
            }

            this.item = item;
            this.offer = offer;
        }

        public String getItem() {
            return item;
        }

        public String getOffer() {
            return offer;
        }

        public String getItemOffer(){
            return  item.concat(offer);
        }

    }

    @RestController
    public static class CountRestController {

        private final QueryableStoreRegistry service;

        public CountRestController(QueryableStoreRegistry service) {
            this.service = service;
        }

        @GetMapping("/item-offer")
        Map<String, String> itemOffer() {

            Map<String, String> counts = new HashMap<>();
            ReadOnlyKeyValueStore<String, String> output = service.getQueryableStoreType("item-offer", QueryableStoreTypes.keyValueStore());

            KeyValueIterator<String, String> all = output.all();

            while (all.hasNext()) {
                KeyValue<String, String> next = all.next();
                counts.put(next.key, next.value);
            }

            return counts;

        }

    }
}
