package xiaoyf.demo.avrokafka.partyv1.consumer;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.*;
import xiaoyf.demo.avrokafka.model.Event;
import xiaoyf.demo.avrokafka.partyv1.serializers.TolerantDeserializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Objects;
import java.util.Properties;

import static xiaoyf.demo.avrokafka.Constants.*;

public class EventListenerV1 {
	public static void main(String[] args) {
		Properties props = new Properties();

		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, EventListenerV1.class.getName());
		props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
		props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);

		// To use 'standard' deserializer
		// props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroDeserializer.class);

		// To use custom tolerant deserializer
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, TolerantDeserializer.class);
		// silentOnUnknownClasses is set to 'true' by default
		props.put("tolerant.silentOnUnknownClasses", false);
		// To enable filter by header
	  props.put("tolerant.headerName", "Type");
	  props.put("tolerant.headerValueRegex", "Sms");

		final Consumer<String, Event> consumer = new KafkaConsumer<String, Event>(props);
		consumer.subscribe(Collections.singletonList(EVENT_TOPIC));

		try {
			while (true) {
				ConsumerRecords<String, Event> records = consumer.poll(Duration.of(1000, ChronoUnit.MILLIS));
				for (ConsumerRecord<String, Event> record : records) {
					Event event = record.value();
					System.out.printf("offset = %d, key = %s, value = %s \n", record.offset(), record.key(), event);

					if (Objects.nonNull(event)) {
						System.out.printf(" - type:%s, payload:%s\n", event.getType(), event.getPayload());
					}
				}
				consumer.commitSync();
			}
		} catch(Exception e) {
			System.err.println("Shit, error!!! " + e.getMessage());
			e.printStackTrace(System.out);
		} finally {
			consumer.close();
		}
	}
}
