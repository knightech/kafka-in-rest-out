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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Pete Knight, Charan Gupta
 */
public class MasterMetaDataProvider {

	public static void main(String... args) throws JsonProcessingException {

		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.RETRIES_CONFIG, 0);
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
		props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

		class Item{
			private String id;
			private String name;

			public Item(){}
			public Item(String id, String name) {
				this.id = id;
				this.name = name;
			}

			public String getId() {
				return id;
			}

			public void setId(String id) {
				this.id = id;
			}

			public String getName() {
				return name;
			}

			public void setName(String name) {
				this.name = name;
			}
		}

		class Offer{
			private String id;
			private String region;

			public Offer(){}

			public Offer(String id, String region) {
				this.id = id;
				this.region = region;
			}

			public String getId() {
				return id;
			}

			public void setId(String id) {
				this.id = id;
			}

			public String getRegion() {
				return region;
			}

			public void setRegion(String region) {
				this.region = region;
			}
		}

		ObjectMapper objectMapper = new ObjectMapper();

		List<KeyValue<String, String>> items = Arrays.asList(
				new KeyValue<>("1001", objectMapper.writeValueAsString(new Item("1001", "Movie"))),
				new KeyValue<>("1002", objectMapper.writeValueAsString(new Item("1002", "Play"))),
				new KeyValue<>("1003", objectMapper.writeValueAsString(new Item("1003", "Series")))
		);

		DefaultKafkaProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(props);
		KafkaTemplate<String, String> itemTemplate = new KafkaTemplate<>(pf, true);
		itemTemplate.setDefaultTopic("items-topic");

		for (KeyValue<String, String> keyValue : items) {
			itemTemplate.sendDefault(keyValue.key, keyValue.value);
		}

		List<KeyValue<String, String>> offers = Arrays.asList(
				new KeyValue<>("1001", objectMapper.writeValueAsString(new Offer("1001", "Horror"))),
				new KeyValue<>("1002", objectMapper.writeValueAsString(new Offer("1002", "Comedy"))),
				new KeyValue<>("1003", objectMapper.writeValueAsString(new Offer("1003", "Action")))
		);

		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

		DefaultKafkaProducerFactory<String, String> pf1 = new DefaultKafkaProducerFactory<>(props);
		KafkaTemplate<String, String> offerTemplate = new KafkaTemplate<>(pf1, true);
		offerTemplate.setDefaultTopic("offers-topic");

		for (KeyValue<String,String> keyValue : offers) {
			offerTemplate.sendDefault(keyValue.key, keyValue.value);
		}

	}

}
