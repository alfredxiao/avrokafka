package xiaoyf.demo.avrokafka.partyv1.consumer;

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.*;
import xiaoyf.demo.avrokafka.model.Transaction;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;

import static xiaoyf.demo.avrokafka.Constants.*;

public class TransactionListenerV1 {
	public static void main(String[] args) {
		Properties props = new Properties();

		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, TransactionListenerV1.class.getName());


		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
		props.put("schema.registry.url", SCHEMA_REGISTRY_URL);
		props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


		final Consumer<String, Transaction> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Collections.singletonList(TRANSACTION_TOPIC));

		try {
			while (true) {
				ConsumerRecords<String, Transaction> records = consumer.poll(Duration.of(1000, ChronoUnit.MILLIS));
				for (ConsumerRecord<String, Transaction> record : records) {
					Transaction tx = record.value();
					System.out.printf("offset = %d, key = %s, value = %s \n", record.offset(), record.key(), tx);
					System.out.printf(" - amount: %s \n", tx.getAmount());
				}
				consumer.commitSync();
			}
		} finally {
			consumer.close();
		}
	}
}
