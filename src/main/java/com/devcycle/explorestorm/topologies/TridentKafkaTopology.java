package com.devcycle.explorestorm.topologies;

import backtype.storm.Config;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import com.devcycle.explorestorm.filter.ExploreLogFilter;
import com.devcycle.explorestorm.function.ExploreTransformMessage;
import com.devcycle.explorestorm.scheme.ExploreScheme;
import storm.kafka.BrokerHosts;
import storm.kafka.ZkHosts;
import storm.kafka.trident.*;
import storm.kafka.trident.mapper.FieldNameBasedTupleToKafkaMapper;
import storm.kafka.trident.selector.DefaultTopicSelector;
import storm.trident.Stream;
import storm.trident.TridentTopology;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by chris howe-jones on 30/09/15.
 */
public class TridentKafkaTopology extends BaseExploreTopology {

    private static final String TOPOLOGY_NAME = "tridentKafkaTopology";
    public static final String KAFKA_ZOOKEEPER_HOST_PORT = "kafka.zookeeper.host.port";
    public static final String KAFKA_TOPIC = "kafka.topic";
    public static final String TRIDENT_KAFKA_SPOUT = "tridentKafkaSpout";
    public static final String TRIDENT_KAFKA_MESSAGE = "tridentKafkaMessage";
    public static final String KAFKA_PUBLISH_TOPIC = "kafka.publish.topic";
    public static final String EXPLORE_TOPOLOGY_PROPERTIES = "explore_topology.properties";
    public static final String REMOTE = "remote";
    public static final String METADATA_BROKER_LIST = "metadata.broker.list";
    public static final String REQUEST_REQUIRED_ACKS = "request.required.acks";
    public static final String SERIALIZER_CLASS = "serializer.class";
    public static final String KAFKA_SERIALIZER_STRING_ENCODER = "kafka.serializer.StringEncoder";

    /**
     * Create a Trident Tooplogy that consumes messages from a Kafka topic and produces messages on a different Kafka topic
     *
     * @param configFileLocation
     * @throws IOException
     */
    public TridentKafkaTopology(String configFileLocation) throws IOException {
        super(configFileLocation);
    }

    /**
     * Main method that creates and submits this topology to Storm.
     *
     * @param args - if the first argument is "remote" then submit this topology to a remote cluster, otherwise run on a local cluster.
     * @throws IOException
     */
    public static void main(String[] args) throws IOException, InterruptedException, InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        String configFileLocation = EXPLORE_TOPOLOGY_PROPERTIES;
        boolean runLocally = true;
        if (args.length >= 1 && args[0].equalsIgnoreCase(REMOTE)) {
            runLocally = false;
        }
        TridentKafkaTopology tridentKafkaTopology
                = new TridentKafkaTopology(configFileLocation);
        tridentKafkaTopology.buildAndSubmit(TOPOLOGY_NAME, runLocally);
    }



    /**
     * Build configuration for this Topology.
     *
     * @return config for the topology
     */
    protected Config buildConfig() {
        Config conf = new Config();
        //set producer properties.
        Properties props = new Properties();
        props.put(METADATA_BROKER_LIST, topologyConfig.getProperty(METADATA_BROKER_LIST));
        props.put(REQUEST_REQUIRED_ACKS, topologyConfig.getProperty(REQUEST_REQUIRED_ACKS));
        props.put(SERIALIZER_CLASS, KAFKA_SERIALIZER_STRING_ENCODER);
        conf.put(TridentKafkaState.KAFKA_BROKER_PROPERTIES, props);
        return conf;
    }



    protected StormTopology buildTopology() {
        TridentTopology topology = new TridentTopology();
        OpaqueTridentKafkaSpout kafkaSpout = buildKafkaSpout();
        ExploreTransformMessage messageTransform = new ExploreTransformMessage();
        Stream tridentStream = topology.newStream(TRIDENT_KAFKA_MESSAGE, kafkaSpout)
                .each(kafkaSpout.getOutputFields(), new ExploreLogFilter())
                .each(kafkaSpout.getOutputFields(), messageTransform, ExploreTransformMessage.getEmittedFields());
        buildKafkaSink(tridentStream);
        return topology.build();
    }

    private void buildKafkaSink(Stream stream) {
        TridentKafkaStateFactory stateFactory = new TridentKafkaStateFactory()
                .withKafkaTopicSelector(new DefaultTopicSelector(topologyConfig.getProperty(KAFKA_PUBLISH_TOPIC)))
                .withTridentTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper(ExploreTransformMessage.key, ExploreTransformMessage.value));
        stream.partitionPersist(stateFactory, ExploreTransformMessage.getEmittedFields(), new TridentKafkaUpdater(), new Fields());
    }

    private OpaqueTridentKafkaSpout buildKafkaSpout() {
        BrokerHosts zk = new ZkHosts(topologyConfig.getProperty(KAFKA_ZOOKEEPER_HOST_PORT));
        TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, topologyConfig.getProperty(KAFKA_TOPIC), TRIDENT_KAFKA_SPOUT);
        spoutConf.scheme = new SchemeAsMultiScheme(new ExploreScheme());
        return new OpaqueTridentKafkaSpout(spoutConf);
    }

}
