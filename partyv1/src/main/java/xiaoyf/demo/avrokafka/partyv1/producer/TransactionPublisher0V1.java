package xiaoyf.demo.avrokafka.partyv1.producer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

import static xiaoyf.demo.avrokafka.Constants.*;

public class TransactionPublisher0V1 {
	public static void main(String[] args) throws Exception {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
		props.put("schema.registry.url", SCHEMA_REGISTRY_URL);
		// props.put("auto.register.schemas", false);
		KafkaProducer producer = new KafkaProducer(props);

		long timestamp = System.currentTimeMillis();

		String key = "key-" + timestamp;
		String userSchema = new String(Files.readAllBytes(Paths.get("./src/main/avro/Transaction.v1.avsc")));

		Schema.Parser parser = new Schema.Parser();
		Schema schema = parser.parse(userSchema);
		GenericRecord avroRecord = new GenericData.Record(schema);
		avroRecord.put("amount", timestamp);

		ProducerRecord<Object, Object> record = new ProducerRecord<>(TRANSACTION_TOPIC, key, avroRecord);
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
