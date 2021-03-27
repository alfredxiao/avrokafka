package xiaoyf.demo.avrokafka.partyv1.producer;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import demo.model.MonetaryActivity;

import java.util.Properties;

import static xiaoyf.demo.avrokafka.Constants.*;

public class MonetaryActivityPublisherV1 {
	public static void main(String[] args) throws Exception {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
		props.put("schema.registry.url", SCHEMA_REGISTRY_URL);
		props.put("auto.register.schemas", true);
		props.put("value.subject.name.strategy", io.confluent.kafka.serializers.subject.RecordNameStrategy.class);
		KafkaProducer<String, MonetaryActivity> producer = new KafkaProducer<>(props);

		long timestamp = System.currentTimeMillis();

		String key = "key-" + timestamp;
		MonetaryActivity tx = MonetaryActivity.newBuilder()
				.setAmount(timestamp)
				.setCategory("Travel")
				.build();

		ProducerRecord<String, MonetaryActivity> record = new ProducerRecord<>(ACTIVITY_TOPIC, key, tx);
		try {
			record.headers().add("Type", "MonetaryActivity".getBytes());
			producer.send(record);
		} catch(SerializationException e) {
			e.printStackTrace();
		} finally {
			producer.flush();
			producer.close();
		}
	}
}
