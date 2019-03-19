package vn.com.techcombank.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;

import vn.com.techcombank.model.Message;

public class Receiver {

	private static final Logger LOGGER = LoggerFactory.getLogger(Receiver.class);

	@KafkaListener(topics = "processed_message", containerFactory = "kafkaListenerContainerFactory")
	public void receive(Message msg, Acknowledgment ack) {
		LOGGER.info("received payload {} in {}", msg.getPayload(), msg.getLanguage());
		ack.acknowledge();
	}

}
