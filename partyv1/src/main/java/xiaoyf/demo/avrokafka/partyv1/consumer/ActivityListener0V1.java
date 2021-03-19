package xiaoyf.demo.avrokafka.partyv1.consumer;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.*;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;

import static xiaoyf.demo.avrokafka.Constants.*;

public class ActivityListener0V1 {
	public static void main(String[] args) {
		Properties props = new Properties();

		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, ActivityListener0V1.class.getName());
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
		props.put("schema.registry.url", SCHEMA_REGISTRY_URL);

		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


		final Consumer<String, GenericRecord> consumer = new KafkaConsumer<String, GenericRecord>(props);
		consumer.subscribe(Collections.singletonList(ACTIVITY_TOPIC));

		try {
			while (true) {
				ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.of(1000, ChronoUnit.MILLIS));
				for (ConsumerRecord<String, GenericRecord> record : records) {
					System.out.printf("offset = %d, key = %s, value = %s \n", record.offset(), record.key(), record.value());
				}
			}
		} finally {
			consumer.close();
		}
	}
}
