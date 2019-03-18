package vn.com.techcombank.util.serde;

import org.apache.kafka.common.serialization.Serde;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import vn.com.techcombank.model.Message;

public class StreamsSerdes {
	
	public static Serde<Message> MessageSerde(){
		return new MessageSerde();
	}

}

final class MessageSerde extends WrapperSerde<Message> {
	public MessageSerde() {
		super(new JsonSerializer<>(), new JsonDeserializer<>(Message.class));
	}
}
