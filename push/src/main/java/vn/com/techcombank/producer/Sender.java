package vn.com.techcombank.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

import vn.com.techcombank.model.Message;

public class Sender {

	private static final Logger LOGGER = LoggerFactory.getLogger(Sender.class);

	@Autowired
	private KafkaTemplate<String, Message> kafkaTemplate;

	public void send(Message msg) {
		LOGGER.info("sending payload = {}", msg.getPayload());
		kafkaTemplate.send("raw_message", msg);
	}

}
