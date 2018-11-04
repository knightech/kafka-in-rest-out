package acme.media.kafkainrestout;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class KafkaEmbeddedTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaEmbeddedTest.class);

    @ClassRule
    public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(2, true, 2, "messages");

    @Test
    public void testSpringKafka() throws Exception {

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("sampleConsumer", "false", embeddedKafka);
        consumerProps.put("auto.offset.reset", "earliest");
        DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
        ContainerProperties containerProps = new ContainerProperties("messages");

        final CountDownLatch latch = new CountDownLatch(4);

        containerProps.setMessageListener((AcknowledgingMessageListener<Integer, String>) (message, ack) -> {
            LOGGER.info("Receiving: " + message);
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            latch.countDown();
        });

        KafkaMessageListenerContainer<Integer, String> container =
                new KafkaMessageListenerContainer<>(cf, containerProps);

        container.setBeanName("sampleConsumer");


        container.start();

        Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
        ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<Integer, String>(senderProps);

        KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
        template.setDefaultTopic("messages");
        template.sendDefault(0, 0, "message1");
        template.sendDefault(0, 1, "message2");
        template.sendDefault(1, 2, "message3");
        template.sendDefault(1, 3, "message4");
        template.flush();
        assertThat(latch.await(20, TimeUnit.SECONDS)).isTrue();
        container.stop();
    }


}