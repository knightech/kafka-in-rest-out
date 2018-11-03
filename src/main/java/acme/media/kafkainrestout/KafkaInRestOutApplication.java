package acme.media.kafkainrestout;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.messaging.handler.annotation.SendTo;


@SpringBootApplication
public class KafkaInRestOutApplication {

    //private final MongoTemplate mongoTemplate;

    public static void main(String[] args) {
        SpringApplication.run(KafkaInRestOutApplication.class, args);
    }

    @EnableBinding(KStreamProcessorX.class)
    public static class KStreamToTableJoinApplication {

        @StreamListener
        @SendTo("item-offer")
        public @Output("item-offer") KStream<String, String> process(
                @Input("item") KTable<String, String> itemTable,
                @Input("offer") KTable<String, String> offerTable,
                @Input("offered-item") KTable<String, String> offerItemTable) {

          KTable<String, String> collated =
                    itemTable.leftJoin(offerItemTable, (leftValue, rightValue) ->
                            "left=" + leftValue + ", right=" + rightValue);

            collated.toStream().peek((key, value) -> System.out.println("key: "+ key + " value: " + value));

            KTable<String, String> join = collated.join(offerTable, (value1, value2) -> "left=" + value1 + ", right=" + value2);

            return join.toStream().peek((key, value) -> System.out.println("key: "+ key + " value: " + value));


        }
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
