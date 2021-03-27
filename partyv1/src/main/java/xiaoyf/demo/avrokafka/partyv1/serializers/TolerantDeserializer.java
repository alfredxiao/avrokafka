package xiaoyf.demo.avrokafka.partyv1.serializers;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.AvroTypeException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;
import java.util.Objects;

import static java.lang.Boolean.TRUE;

/**
 * TolerantDeserializer has additional processing to tolerate some error scenarios:
 * 1. When the entire record's corresponding Java class is not found, return null
 * 2. When a unknown class/type found in union record, return null
 * 3. Filter/accept messages with matching headers (by regex matching) only, return null for those mismatch
 */
public class TolerantDeserializer implements Deserializer<Object> {
	protected KafkaAvroDeserializer worker;
	protected boolean silentOnUnknownClasses;   // when 'true': return null for unknown classes or unknown union type
	protected String headerName;                // if set: check header values under this name
	protected String headerValueRegex;          // only relevant when 'checkHeaderName' is also set. when both set,
																							// accept only certain messages whose header name and value match
																							// our configuration, otherwise return null

	public TolerantDeserializer() {
		worker = new KafkaAvroDeserializer();
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		headerName = String.valueOf(configs.get("tolerant.headerName"));
		headerValueRegex = String.valueOf(configs.get("tolerant.headerValueRegex"));

		Object flag = configs.get("tolerant.silentOnUnknownClasses");
		silentOnUnknownClasses = flag != null && (TRUE.equals(flag) || "true".equalsIgnoreCase(flag.toString()));

		// perhaps better remove these custom config entries before passing on
		worker.configure(configs, isKey);
	}

	@Override
	public Object deserialize(String topic, byte[] data) {
		try {
			return worker.deserialize(topic, data);
		} catch(SerializationException e) {
			return handleUnknownClassesOrRethrow(e);
		}
	}

	@Override
	public Object deserialize(String topic, Headers headers, byte[] data) {
		try {
			if (!acceptsMessageByHeader(headers)) {
				System.out.println("TolerantDeserializer returns null for this message due to headers not being accepted:" + headersToString(headers));
				return null;
			}

			return worker.deserialize(topic, headers, data);
		} catch(SerializationException e) {
			return handleUnknownClassesOrRethrow(e);
		}
	}

	protected Object handleUnknownClassesOrRethrow(SerializationException e) throws SerializationException {
		if (!silentOnUnknownClasses) throw e;

		Throwable t = e;

		int depth = 0;
		while (depth < 5 && Objects.nonNull(t)) {
			if (couldNotFindClass(t.getMessage()) || expectingUnionButFoundSomethingElse(t)) {
				System.out.println("TolerantDeserializer returns null due to unknown class/type");
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

	protected boolean acceptsMessageByHeader(Headers headers) {
		if (Objects.isNull(headers)) return true;

		for (Header header : headers.headers(headerName)) {
			String value = new String(header.value());
			if (!value.matches(headerValueRegex)) {
				return false;
			}
		};

		return true;
	}

	@Override
	public void close() {
		worker.close();
	}

	private String headersToString(Headers headers) {
		StringBuilder builder = new StringBuilder();
		headers.forEach(header -> {
			builder.append(header.key()).append("=").append(new String(header.value()));
		});

		return builder.toString();
	}
}
