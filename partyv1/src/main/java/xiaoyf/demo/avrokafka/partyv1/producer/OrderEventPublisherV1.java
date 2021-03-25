package xiaoyf.demo.avrokafka.partyv1.producer;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import xiaoyf.demo.avrokafka.model.OrderDeliveryEvent;
import xiaoyf.demo.avrokafka.model.PlaceOrderEvent;

import java.util.Properties;

import static xiaoyf.demo.avrokafka.Constants.*;

public class OrderEventPublisherV1 {
	public static void main(String[] args) throws Exception {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
		props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
		props.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false);
		props.put(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, true);
		KafkaProducer<String, Object> producer = new KafkaProducer<>(props);

		long timestamp = System.currentTimeMillis();

		String key1 = "key1-" + timestamp;
		PlaceOrderEvent placeOrder = PlaceOrderEvent.newBuilder()
								.setOrderId("order-" + timestamp)
								.setProductName("iPhone")
								.setQuantity(1)
								.build();

		String key2 = "key2-" + timestamp;
		OrderDeliveryEvent orderDelivery =  OrderDeliveryEvent.newBuilder()
								.setOrderId("order-" + timestamp)
								.setWhen(timestamp)
								.setDelivered(true)
								.build();

		ProducerRecord<String, Object> placeOrderRecord = new ProducerRecord<>(ORDER_EVENT_TOPIC, key1, placeOrder);
		ProducerRecord<String, Object> orderDeliveryRecord = new ProducerRecord<>(ORDER_EVENT_TOPIC, key2, orderDelivery);
		try {
			producer.send(placeOrderRecord);
			producer.send(orderDeliveryRecord);
		} catch(SerializationException e) {
			e.printStackTrace();
		} finally {
			producer.flush();
			producer.close();
		}
	}
}

