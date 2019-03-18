package vn.com.techcombank.util.serde;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

public class WrapperSerde<T> implements Serde<T> {

	private JsonSerializer<T> serializer;
	private JsonDeserializer<T> deserializer;

	WrapperSerde(JsonSerializer<T> serializer, JsonDeserializer<T> deserializer) {
		this.serializer = serializer;
		this.deserializer = deserializer;
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	@Override
	public Serializer<T> serializer() {
		return serializer;
	}

	@Override
	public Deserializer<T> deserializer() {
		return deserializer;
	}

}
