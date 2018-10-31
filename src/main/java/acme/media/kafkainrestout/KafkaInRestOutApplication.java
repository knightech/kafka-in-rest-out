package acme.media.kafkainrestout;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.kafka.streams.QueryableStoreRegistry;
import org.springframework.cloud.stream.binder.kafka.streams.annotations.KafkaStreamsProcessor;
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
		public KStream<String, Long> process(@Input("input") KStream<String, Long> userClicksStream,
                                             @Input("helloX") KTable<String, String> userRegionsTable) {

			return userClicksStream
					.leftJoin(userRegionsTable,
							(clicks, region) -> new RegionWithClicks(region == null ? "UNKNOWN" : region, clicks),
							Joined.with(Serdes.String(), Serdes.Long(), null))
					.map((user, regionWithClicks) -> new KeyValue<>(regionWithClicks.getRegion(), regionWithClicks.getClicks()))
					.groupByKey(Serialized.with(Serdes.String(), Serdes.Long()))
					.count(Materialized.as("mytable"))
					//.reduce((firstClicks, secondClicks) -> firstClicks + secondClicks)
					.toStream();
		}

/*		@StreamListener
		public void process(@Input("input") KStream<String, Long> userClicksStream,
											 @Input("helloX") KTable<String, String> userRegionsTable) {

			 userClicksStream
					.leftJoin(userRegionsTable,
							(clicks, region) -> new RegionWithClicks(region == null ? "UNKNOWN" : region, clicks),
							Joined.with(Serdes.String(), Serdes.Long(), null))
					.map((user, regionWithClicks) -> new KeyValue<>(regionWithClicks.getRegion(), regionWithClicks.getClicks()))
					.groupByKey(Serialized.with(Serdes.String(), Serdes.Long()))
					 .reduce((firstClicks, secondClicks) -> firstClicks + secondClicks)
					 .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("output"))

					.toStream().to("output");

		}*/
	}





	interface KStreamProcessorX extends KafkaStreamsProcessor {

		@Input("helloX")
		KTable<?, ?> helloX();
	}

	private static final class RegionWithClicks {

		private final String region;
		private final long clicks;

		public RegionWithClicks(String region, long clicks) {
			if (region == null || region.isEmpty()) {
				throw new IllegalArgumentException("region must be set");
			}
			if (clicks < 0) {
				throw new IllegalArgumentException("clicks must not be negative");
			}
			this.region = region;
			this.clicks = clicks;
		}

		public String getRegion() {
			return region;
		}

		public long getClicks() {
			return clicks;
		}

	}

	@RestController
	public static class CountRestController {

		private final QueryableStoreRegistry service;

		public CountRestController(QueryableStoreRegistry service) {
			this.service = service;
		}

		@GetMapping("/counts")
		Map<String, Long> counts(){

			Map<String, Long> counts = new HashMap<>();
			ReadOnlyKeyValueStore<String, Long> output = service.getQueryableStoreType("mytable", QueryableStoreTypes.keyValueStore());

			KeyValueIterator<String, Long> all = output.all();
			while(all.hasNext()){
				KeyValue<String, Long> next = all.next();
				counts.put(next.key, next.value);
			}

			return counts;

		}

	}
}
