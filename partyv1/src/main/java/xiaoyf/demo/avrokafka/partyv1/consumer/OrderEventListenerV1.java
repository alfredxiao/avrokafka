package xiaoyf.demo.avrokafka.partyv1.consumer;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.*;
import demo.model.OrderDeliveryEvent;
import demo.model.PlaceOrderEvent;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;

import static xiaoyf.demo.avrokafka.Constants.*;


public class OrderEventListenerV1 {
	public static void main(String[] args) {
		Properties props = new Properties();

		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, OrderEventListenerV1.class.getName()+"12");
		props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
		props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);
		 props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroDeserializer.class);

		final Consumer<String, Object> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Collections.singletonList(ORDER_EVENT_TOPIC));

		try {
			while (true) {
				ConsumerRecords<String, Object> records = consumer.poll(Duration.of(1000, ChronoUnit.MILLIS));
				for (ConsumerRecord<String, Object> record : records) {
					Object orderEvent = record.value();
					System.out.printf("offset = %d, key = %s, value = %s \n", record.offset(), record.key(), orderEvent);

					if (orderEvent instanceof PlaceOrderEvent) {
						PlaceOrderEvent placeOrderEvent = (PlaceOrderEvent) orderEvent;
						System.out.printf(" - name:%s, quantity:%d\n", placeOrderEvent.getProductName(), placeOrderEvent.getQuantity());
					}

					if (orderEvent instanceof OrderDeliveryEvent) {
						OrderDeliveryEvent orderDeliveryEvent = (OrderDeliveryEvent) orderEvent;
						System.out.printf(" - delivered:%s, when:%d\n", orderDeliveryEvent.getDelivered(), orderDeliveryEvent.getWhen());
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
