package com.devcycle.explorestorm.topologies;

import backtype.storm.Config;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.Scheme;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import com.devcycle.explorestorm.filter.ExploreLogFilter;
import com.devcycle.explorestorm.filter.RemoveInvalidMessages;
import com.devcycle.explorestorm.function.ExploreTransformMessage;
import com.devcycle.explorestorm.function.ParseCBSMessage;
import com.devcycle.explorestorm.function.PrintFunction;
import com.devcycle.explorestorm.scheme.CBSKafkaScheme;
import com.devcycle.explorestorm.util.HBaseConfigBuilder;
import com.devcycle.explorestorm.util.StormRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentTopology;

import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;

import static com.devcycle.explorestorm.topologies.HBaseConfig.HBASE_CONFIG;
import static com.devcycle.explorestorm.topologies.KafkaConfig.KAFKA_ZOOKEEPER_HOST_PORT;
import static com.devcycle.explorestorm.util.StormRunner.REMOTE;

/**
 * Topology to read CBS messages from kafka and persist them to HBase.
 * <p/>
 * Created by chrishowe-jones on 21/10/15.
 */
public class PersistCBSTopology extends BaseExploreTopology {


    public static final String STREAM_NAME = "CbsHBaseStream";
    public static final String EXPLORE_TOPOLOGY_PROPERTIES = "explore_topology.properties";

    private static final Logger LOG = LoggerFactory.getLogger(PersistCBSTopology.class);
    private static final java.lang.String KAFKA_TOPIC = "CBSTopic";
    private static final String TRIDENT_KAFKA_SPOUT = "CBSMessageSpout";
    private static final String TOPOLOGY_NAME = "persistCBSTopology";
    private HBaseConfigBuilder hbaseConfigBuilder;
    private Scheme cbsKafkaScheme;
    private OpaqueTridentKafkaSpout kafkaSpout;

    /**
     * Create PersistCBSTopology.
     *
     * @param configFileLocation path to config properties file.
     * @throws IOException
     */
    public PersistCBSTopology(String configFileLocation) throws IOException {
        super(configFileLocation);
    }

    public PersistCBSTopology(Properties configProperties) {
        super(configProperties);
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
        String hbaseRoot;
        PersistCBSTopology persistCBSTopology;
        if (args.length >= 1 && args[0].trim().equalsIgnoreCase(REMOTE)) {
            runLocally = false;
        }
        persistCBSTopology
                    = new PersistCBSTopology(configFileLocation);

        persistCBSTopology.buildAndSubmit(TOPOLOGY_NAME, runLocally);
    }

    public Scheme getCBSKafkaScheme() {
        if (cbsKafkaScheme == null)
            cbsKafkaScheme = new CBSKafkaScheme();
        return cbsKafkaScheme;
    }

    @Override
    protected Config buildConfig() {
        final Config config = new Config();
        config.put(HBASE_CONFIG.toString(), buildHBaseConfig());

        return config;
    }

    @Override
    protected StormTopology buildTopology() {
        TridentTopology topology = new TridentTopology();
        Scheme cbsKafkaScheme = getCBSKafkaScheme();

        OpaqueTridentKafkaSpout kafkaSpout = buildKafkaSpout(cbsKafkaScheme);

        topology.newStream(STREAM_NAME, kafkaSpout)
                .each(kafkaSpout.getOutputFields(), new ParseCBSMessage(CBSKafkaScheme.FIELD_JSON_MESSAGE), ParseCBSMessage.getEmittedFields())
                .each(ParseCBSMessage.getEmittedFields(), new RemoveInvalidMessages())
                .each(ParseCBSMessage.getEmittedFields(), new PrintFunction(), new Fields());
        return topology.build();
    }

    HBaseConfigBuilder getHBaseConfigBuilder() {
        if (hbaseConfigBuilder == null)
            this.hbaseConfigBuilder = new HBaseConfigBuilder();
        return hbaseConfigBuilder;
    }

    void setHBaseConfigBuilder(HBaseConfigBuilder hbaseConfigBuilder) {
        this.hbaseConfigBuilder = hbaseConfigBuilder;
    }

    void setCbsKafkaScheme(CBSKafkaScheme cbsKafkaScheme) {
        this.cbsKafkaScheme = cbsKafkaScheme;
    }

    void setKafkaSpout(OpaqueTridentKafkaSpout spout) {
        this.kafkaSpout = spout;
    }

    private OpaqueTridentKafkaSpout getKafkaSpoutInstance(TridentKafkaConfig spoutConf) {
        if (kafkaSpout == null) {
            kafkaSpout = new OpaqueTridentKafkaSpout(spoutConf);
        }
        return kafkaSpout;
    }

    private HashMap<String, Object> buildHBaseConfig() {
        return getHBaseConfigBuilder().getHBaseConfig(topologyConfig);
    }

    private OpaqueTridentKafkaSpout buildKafkaSpout(Scheme cbsKafkaScheme) {
        LOG.info("Build Kafka Spout");
        BrokerHosts zk = new ZkHosts(topologyConfig.getProperty(KAFKA_ZOOKEEPER_HOST_PORT.toString()));
        TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, topologyConfig.getProperty(KAFKA_TOPIC), TRIDENT_KAFKA_SPOUT);
        spoutConf.scheme = new SchemeAsMultiScheme(cbsKafkaScheme);
        return getKafkaSpoutInstance(spoutConf);
    }

}
