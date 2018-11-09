/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package acme.media.kafkainrestout;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Pete Knight, Charan Gupta
 */
public class MasterMetaDataProvider {

	private ObjectMapper objectMapper = new ObjectMapper();


	public static void main(String[] args) throws IOException {
		new MasterMetaDataProvider().load();
	}

	private void load() throws IOException {

        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        List<KeyValue<String, String>> offeredItems = Arrays.asList(
                new KeyValue<>("3003", getJsonContent("offered-item.json")),
                new KeyValue<>("6006", getJsonContent("offered-item2.json")),
                new KeyValue<>("9009", getJsonContent("offered-item3.json")),
                new KeyValue<>("2002", getJsonContent("offered-item4.json")),
                new KeyValue<>("4004", getJsonContent("offered-item5.json"))

        );

        sendOfferItems(props, offeredItems);
        List<KeyValue<String, String>> offers = Arrays.asList(
                new KeyValue<>("5001", getJsonContent("offer.json")),
                new KeyValue<>("6001", getJsonContent("offer2.json")),
                new KeyValue<>("7001", getJsonContent("offer3.json")),
                new KeyValue<>("8001", getJsonContent("offer4.json")),
                new KeyValue<>("9001", getJsonContent("offer5.json"))
        );
        sendOffers(props, offers);


        List<KeyValue<String, String>> items = Arrays.asList(
                new KeyValue<>("1001", getJsonContent("item.json")),
                new KeyValue<>("1002", getJsonContent("item2.json")),
                new KeyValue<>("1003", getJsonContent("item3.json"))
        );
        sendItems(props, items);
    }

    private void sendOfferItems(Map<String, Object> props, Iterable<? extends KeyValue<String, String>> offeredItems) {

        DefaultKafkaProducerFactory<String, String> pf2 = new DefaultKafkaProducerFactory<>(props);
        KafkaTemplate<String, String> offeredItemsTemplate = new KafkaTemplate<>(pf2, true);
        offeredItemsTemplate.setDefaultTopic("offered-items");

        for (KeyValue<String,String> keyValue : offeredItems) {
            offeredItemsTemplate.sendDefault(keyValue.key, keyValue.value);
        }
    }

    private void sendItems(Map<String, Object> props, Iterable<? extends KeyValue<String, String>> items) {

        DefaultKafkaProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(props);
        KafkaTemplate<String, String> itemTemplate = new KafkaTemplate<>(pf, true);
        itemTemplate.setDefaultTopic("items");

        for (KeyValue<String,String> keyValue : items) {
            itemTemplate.sendDefault(keyValue.key, keyValue.value);
        }
    }

    private void sendOffers(Map<String, Object> props, Iterable<? extends KeyValue<String, String>> offers) {

        DefaultKafkaProducerFactory<String, String> pf1 = new DefaultKafkaProducerFactory<>(props);
        KafkaTemplate<String, String> offerTemplate = new KafkaTemplate<>(pf1, true);
        offerTemplate.setDefaultTopic("offers");

        for (KeyValue<String,String> keyValue : offers) {
            offerTemplate.sendDefault(keyValue.key, keyValue.value);
        }
    }

    private String getJsonContent(final String name) throws IOException {
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource(name).getFile());
        return FileUtils.readFileToString(file, "UTF-8");
    }


}
