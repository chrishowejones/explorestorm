package com.devcycle.explorestorm.topologies;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import com.devcycle.explorestorm.bolt.LogExploreEventsBolt;
import com.devcycle.explorestorm.scheme.ExploreScheme;
import com.devcycle.explorestorm.util.StormRunner;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Created by chrishowe-jones on 17/09/15.
 */
public class ExploreTopology extends BaseExploreTopology {

    private static final String KAFKA_SPOUT_ID = "kafkaExploreSpout";
    private static final String LOG_EXPLORE_BOLT_ID = "logExploreEventBolt";
    private static final int DEFAULT_RUNTIME_IN_SECONDS = 6000;
    private final TopologyBuilder builder;
    private final String topologyName;
    private final int runtimeInSeconds;


    public ExploreTopology(final String configFileLocation, final String topologyName) throws IOException {
        super(configFileLocation);
        builder = new TopologyBuilder();
        this.topologyName = topologyName;
        runtimeInSeconds = DEFAULT_RUNTIME_IN_SECONDS;
    }

    public void configureKafkaSpout(TopologyBuilder builder) {
        KafkaSpout kafkaSpout = new KafkaSpout(constructKafkaSpoutConf());
        int spoutCount = Integer.valueOf(topologyConfig.getProperty("spout.thread.count"));
        builder.setSpout(KAFKA_SPOUT_ID, kafkaSpout);
    }

    private SpoutConfig constructKafkaSpoutConf() {
        BrokerHosts hosts = new ZkHosts(topologyConfig.getProperty("kafka.zookeeper.host.port"));
        String topic = topologyConfig.getProperty("kafka.topic");
        String zkRoot = topologyConfig.getProperty("kafka.zkRoot");
        String consumerGroupId = "StormSpout";

        SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRoot, consumerGroupId);

        /* Custom Scheme that will take Kafka message of single event
         * and emit a 2-tuple consisting of id and eventMessage.  */
        spoutConfig.scheme = new SchemeAsMultiScheme(new ExploreScheme());

        return spoutConfig;

    }

    public void configureLogExploreEventBolt(TopologyBuilder builder)
    {
        LogExploreEventsBolt logBolt = new LogExploreEventsBolt();
        builder.setBolt(LOG_EXPLORE_BOLT_ID, logBolt).globalGrouping(KAFKA_SPOUT_ID);
    }

    public void runLocally(Config config) throws InterruptedException {
        StormRunner.runTopologyLocally(builder.createTopology(), topologyName, config, runtimeInSeconds);
    }

    public void runRemotely(Config config) throws Exception {
        StormRunner.runTopologyRemotely(builder.createTopology(), topologyName, config);
    }

    private void buildAndSubmit(boolean runLocally) throws Exception
    {
        configureKafkaSpout(builder);
        configureLogExploreEventBolt(builder);


        Config conf = new Config();
        conf.setDebug(true);

        if(runLocally) {
            runLocally(conf);
        } else {
            runRemotely(conf);
        }
    }

    public static void main(String[] args) throws Exception
    {
        String configFileLocation = "explore_topology.properties";
        String topologyName = "explore-log";
        boolean runLocally = true;
        if (args.length >= 1 && args[0].equalsIgnoreCase("remote")) {
            runLocally = false;
        }
        ExploreTopology exploreTopology
                = new ExploreTopology(configFileLocation, topologyName);
        exploreTopology.buildAndSubmit(runLocally);
    }
}
