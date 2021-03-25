package xiaoyf.demo.avrokafka.partyv1.producer;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import xiaoyf.demo.avrokafka.model.Transaction;

import java.util.Properties;

import static xiaoyf.demo.avrokafka.Constants.*;

public class TransactionPublisherV1 {
	public static void main(String[] args) throws Exception {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
		props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
		props.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, true);
		KafkaProducer<String, Transaction> producer = new KafkaProducer<>(props);

		long timestamp = System.currentTimeMillis();

		String key = "key-" + timestamp;
		Transaction tx = Transaction.newBuilder()
				.setAmount(timestamp)
				.build();

		ProducerRecord<String, Transaction> record = new ProducerRecord<>(TRANSACTION_TOPIC, key, tx);
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
