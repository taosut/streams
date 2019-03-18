package vn.com.techcombank.processor;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;

import vn.com.techcombank.model.Message;
import vn.com.techcombank.util.serde.StreamsSerdes;

@Configuration
@EnableKafka
@EnableKafkaStreams
public class MessageKafkaStreams {
	private static final Logger LOGGER = LoggerFactory.getLogger(MessageKafkaStreams.class);

	@Value("${kafka.bootstrap-servers}")
	private String bootstrapServers;

	@Value("${kafka.topic.streamRawDataTopic}")
	private String rawDataTopic;

	@Value("${kafka.topic.streamProcessedDataTopic}")
	private String processedDataTopic;

	@Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
	public StreamsConfig kafkaStreamsConfigs() {
		Map<String, Object> props = new HashMap<>();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "messageKafkaStreams");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		// optional
		props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());
		return new StreamsConfig(props);
	}

	@Bean
	public KStream<String, Message> kafkaStream(StreamsBuilder kStreamBuilder) {
		KStream<String, Message> stream = kStreamBuilder.stream(rawDataTopic,
				Consumed.with(Serdes.String(), StreamsSerdes.MessageSerde()));
		KStream<String, Message> filteredStream = stream.filter((key, value) -> "EN".equals(value.getLanguage()));
		filteredStream.mapValues(m -> {
			Message o = new Message();
			o.setLanguage(m.getLanguage());
			o.setPayload(new StringBuilder(m.getPayload()).toString().toUpperCase());
			return o;
		});
		filteredStream.to(processedDataTopic, Produced.with(Serdes.String(), StreamsSerdes.MessageSerde()));
		LOGGER.info("Stream started here...");
		return filteredStream;
	}

}
