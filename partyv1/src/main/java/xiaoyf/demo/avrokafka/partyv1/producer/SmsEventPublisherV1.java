package xiaoyf.demo.avrokafka.partyv1.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import xiaoyf.demo.avrokafka.model.Event;
import xiaoyf.demo.avrokafka.model.Sms;

import java.util.Properties;

import static xiaoyf.demo.avrokafka.Constants.*;

public class SmsEventPublisherV1 {
	public static void main(String[] args) throws Exception {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
		props.put("schema.registry.url", SCHEMA_REGISTRY_URL);
		props.put("auto.register.schemas", "true");
		KafkaProducer<String, Event> producer = new KafkaProducer<>(props);

		long timestamp = System.currentTimeMillis();

		String key = "key-" + timestamp;
		Event event = Event.newBuilder()
				.setType("Sms")
				.setEventId("event-" + timestamp)
				.setPayload(
						Sms.newBuilder()
								.setPhoneNumber("100888")
								.setText("hi-v1-" + timestamp)
								.build()
				).build();

		ProducerRecord<String, Event> record = new ProducerRecord<>(EVENT_TOPIC, key, event);
		try {
			producer.send(record);
		} catch(SerializationException e) {
			e.printStackTrace();
		} finally {
			producer.flush();
			producer.close();
		}
	}
}
