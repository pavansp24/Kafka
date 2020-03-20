package com.kafka.admin;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;

public class KafkaAdministration {
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		Map<String, Object> conf = new HashMap<String, Object>();
		conf.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		conf.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
		AdminClient client = AdminClient.create(conf);

		listTopics(client);

		createTopic(client);

		client.listConsumerGroups();

		getConsumerGroupOffset(client);

		describeTopic(client);

		updateTopic(client);

		deleteTopic(client);

		getClusterDetails(client);
		

	}

	private static void getConsumerGroupOffset(AdminClient client) throws InterruptedException, ExecutionException {
		ListConsumerGroupOffsetsResult result = client.listConsumerGroupOffsets("grp-1");
		Map<TopicPartition, OffsetAndMetadata> maps = result.partitionsToOffsetAndMetadata().get();
		System.out.println("Offset ->" + maps.toString());
	}

	private static void getClusterDetails(AdminClient client) throws InterruptedException, ExecutionException {
		DescribeClusterResult clusterResult = client.describeCluster();
		System.out.println(clusterResult.controller().get().id());
		System.out.println(clusterResult.clusterId().get());

	}

	private static void describeTopic(AdminClient client) throws InterruptedException, ExecutionException {

		DescribeTopicsResult result = client.describeTopics(Collections.singleton("kafka-admin"));

		KafkaFuture<Map<String, TopicDescription>> output = result.all();

		System.out.println(output.get());
	}

	/**
	 * List all the topics
	 * @param client
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	private static void listTopics(AdminClient client) throws InterruptedException, ExecutionException {
		ListTopicsResult ltr = client.listTopics();
		KafkaFuture<Set<String>> names = ltr.names();
		names.get().forEach(name -> {
			ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, name);
			DescribeConfigsResult describeConfigsResult = client.describeConfigs(Collections.singleton(resource));
			Map<ConfigResource, Config> config;
			try {
				config = describeConfigsResult.all().get();
				System.out.println("Configuration of Topic -->" + config);
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}

		});
	}

	/**
	 * Delete the topic
	 * @param client
	 */
	private static void deleteTopic(AdminClient client) {
		KafkaFuture<Void> future = client.deleteTopics(Collections.singleton("kafka-admin")).all();
		try {
			future.get();
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
	}

	private static void createTopic(AdminClient client) {
		int partitions = 1;
		short replicationFactor = 1;
		try {
			KafkaFuture<Void> future = client.createTopics
														(Collections.singleton(
																				new NewTopic("kafka-admin", partitions, replicationFactor)),
																				new CreateTopicsOptions().timeoutMs(8000)).all();
			future.get();
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
	}

	public static void updateTopic(AdminClient client) throws InterruptedException, ExecutionException {
		ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "kafka-admin");
		DescribeConfigsResult describeConfigsResult = client.describeConfigs(Collections.singleton(resource));
		Map<ConfigResource, Config> config;
		config = describeConfigsResult.all().get();

		System.out.println("before -->" + config);

		ConfigEntry retentionEntry = new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "40000");
		Map<ConfigResource, Config> updateConfig = new HashMap<ConfigResource, Config>();
		updateConfig.put(resource, new Config(Collections.singleton(retentionEntry)));

		AlterConfigsResult alterConfigsResult = client.alterConfigs(updateConfig);
		alterConfigsResult.all();

		describeConfigsResult = client.describeConfigs(Collections.singleton(resource));

		config = describeConfigsResult.all().get();
		System.out.println("after -->" + config);
	}
}
