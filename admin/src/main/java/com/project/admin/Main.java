package com.project.admin;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.security.InvalidParameterException;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;

public class Main {
	private static final String defaultTopicNumPartitions = "8"; // to change accordingly to pool size
	private static final String defaultTopicReplicationFactor = "1";

	public static void main(String[] args)
			throws ExecutionException, InterruptedException, IOException, URISyntaxException {
		Properties props = new Properties();
		if (args.length != 1) {
			throw new InvalidParameterException("Should pass librdkafka config path as argument");
		}
		var librdkafkaConfig = args[0];
		var configPath = Paths.get("").resolve(librdkafkaConfig).normalize().toString();
		props.load(new FileInputStream(configPath));
		props.list(System.out);

		AdminClient adminClient = AdminClient.create(props);

		var topicNumPartitions = Integer.valueOf(
				Stream.of(System.getenv("TOPIC_NUM_PARTITIONS")).filter(Objects::nonNull).findFirst()
						.orElse(defaultTopicNumPartitions));
		var topicReplicationFactor = Short.valueOf(
				Stream.of(System.getenv("TOPIC_REPLICATION_FACTOR")).filter(Objects::nonNull).findFirst()
						.orElse(defaultTopicReplicationFactor));
		try {
			adminClient.createTopics(List.of(
					new NewTopic("calculus", topicNumPartitions, topicReplicationFactor),
					new NewTopic("image-compression", topicNumPartitions, topicReplicationFactor),
					new NewTopic("word-count", topicNumPartitions, topicReplicationFactor),
					new NewTopic("error-result", topicNumPartitions, topicReplicationFactor),
					new NewTopic("calculus-result", topicNumPartitions, topicReplicationFactor),
					new NewTopic("image-compression-result", topicNumPartitions, topicReplicationFactor),
					new NewTopic("word-count-result", topicNumPartitions, topicReplicationFactor))).all().get();
		} catch (ExecutionException e) {
			var cause = e.getCause();
			if (!(cause instanceof TopicExistsException)) {
				throw e;
			}
			e.printStackTrace();
		}

		System.out.println("Launched with success!");
	}
}
