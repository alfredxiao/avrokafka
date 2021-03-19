package xiaoyf.demo.avrokafka.partyv1.serializers;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.AvroTypeException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;
import java.util.Objects;

public class TolerantDeserializer implements Deserializer<Object> {
	KafkaAvroDeserializer worker;

	public TolerantDeserializer() {
		worker = new KafkaAvroDeserializer();
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		worker.configure(configs, isKey);
	}

	@Override
	public Object deserialize(String topic, byte[] data) {
		try {
			return worker.deserialize(topic, data);
		} catch(SerializationException e) {
			return isDueToUnknownSpecificClass(e);
		}
	}

	@Override
	public Object deserialize(String topic, Headers headers, byte[] data) {
		try {
			return worker.deserialize(topic, headers, data);
		} catch(SerializationException e) {
			return isDueToUnknownSpecificClass(e);
		}
	}

	protected Object isDueToUnknownSpecificClass(SerializationException e) throws SerializationException {
		Throwable t = e;

		int depth = 0;
		while (depth < 5 && Objects.nonNull(t)) {
			if (couldNotFindClass(t.getMessage()) || expectingUnionButFoundSomethingElse(t)) {
				return null;
			}

			t = t.getCause();
			depth++;
		}

		throw e;
	}

	private boolean couldNotFindClass(String message) {
		return Objects.nonNull(message) &&
				message.startsWith("Could not find class ") &&
				message.endsWith("whilst finding reader's schema for a SpecificRecord.");
	}

	private boolean expectingUnionButFoundSomethingElse(Throwable t) {
		if (t instanceof AvroTypeException) {
			String message = t.getMessage();
			return Objects.nonNull(message) &&
					message.startsWith("Found ") &&
					message.endsWith("expecting union");
		}

		return false;
	}

	@Override
	public void close() {
		worker.close();
	}
}
