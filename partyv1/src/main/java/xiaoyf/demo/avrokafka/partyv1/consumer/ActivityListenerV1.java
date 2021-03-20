package xiaoyf.demo.avrokafka.partyv1.consumer;

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.*;
import xiaoyf.demo.avrokafka.model.MonetaryActivity;
import xiaoyf.demo.avrokafka.model.NonMonetaryActivity;
import xiaoyf.demo.avrokafka.partyv1.serializers.TolerantDeserializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;

import static xiaoyf.demo.avrokafka.Constants.*;

public class ActivityListenerV1 {
	public static void main(String[] args) {
		Properties props = new Properties();

		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put("schema.registry.url", SCHEMA_REGISTRY_URL);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, ActivityListenerV1.class.getName());
		props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);

		// To use 'standard' deserializer
		// props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroDeserializer.class);

		// To use custom tolerant deserializer
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, TolerantDeserializer.class);
		// silentOnUnknownClasses is set to 'true' by default
		props.put("tolerant.silentOnUnknownClasses", true);
		// To enable filter by header
		// props.put("tolerant.headerName", "Type");
		// props.put("tolerant.headerValueRegex", "MonetaryActivity|NonMonetaryActivity");

		final Consumer<String, SpecificRecordBase> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Collections.singletonList(ACTIVITY_TOPIC));

		try {
			while (true) {
				ConsumerRecords<String, SpecificRecordBase> records = consumer.poll(Duration.of(1000, ChronoUnit.MILLIS));
				for (ConsumerRecord<String, SpecificRecordBase> record : records) {
					SpecificRecordBase activity = record.value();
					System.out.printf("offset = %d, key = %s, value = %s \n", record.offset(), record.key(), activity);

					if (activity instanceof MonetaryActivity) {
						System.out.printf(" - activity: amount: %s, category: %s \n", activity.get("amount"), activity.get("category"));
					}

					if (activity instanceof NonMonetaryActivity) {
						System.out.printf(" - activity: action: %s, when: %s \n", activity.get("action"), activity.get("when"));
					}
				}
				consumer.commitSync();
			}
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			consumer.close();
		}
	}
}
