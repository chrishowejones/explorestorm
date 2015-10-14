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
import com.devcycle.explorestorm.mapper.ExploreMessageValueMapper;
import com.devcycle.explorestorm.scheme.ExploreScheme;
import org.apache.hadoop.hbase.client.Durability;
import org.slf4j.LoggerFactory;
import org.apache.storm.hbase.bolt.mapper.HBaseValueMapper;
import org.apache.storm.hbase.trident.mapper.SimpleTridentHBaseMapper;
import org.apache.storm.hbase.trident.mapper.TridentHBaseMapper;
import org.apache.storm.hbase.trident.state.HBaseState;
import org.apache.storm.hbase.trident.state.HBaseStateFactory;
import org.apache.storm.hbase.trident.state.HBaseUpdater;
import org.slf4j.Logger;
import storm.kafka.BrokerHosts;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.state.StateFactory;

import java.io.IOException;
import java.util.HashMap;

/**
 * Topology to consume data from Kafka spout and persist in HBase.
 * <p/>
 * Created by chris howe-jones on 05/10/15.
 */
public class KafkaHBaseTopology extends BaseExploreTopology {

    private static final String HBASE_CONFIG = "hbase.config";
    private Logger LOG = LoggerFactory.getLogger(KafkaHBaseTopology.class);

    private static final String TOPOLOGY_NAME = "kafkaHBaseTopology";
    public static final String KAFKA_ZOOKEEPER_HOST_PORT = "kafka.zookeeper.host.port";
    public static final String KAFKA_TOPIC = "kafka.topic";
    public static final String TRIDENT_KAFKA_SPOUT = "hbaseKafkaSpout";
    public static final String TRIDENT_KAFKA_MESSAGE = "hbaseKafkaMessage";

    public static final String EXPLORE_TOPOLOGY_PROPERTIES = "explore_topology.properties";
    public static final String REMOTE = "remote";
    public static final String HBASE_ROOT = "hbase.root";
    private final String hbaseRoot;


    public KafkaHBaseTopology(String configFileLocation
    ) throws IOException {
        super(configFileLocation);
        this.hbaseRoot = topologyConfig.getProperty(HBASE_ROOT);
    }

    public KafkaHBaseTopology(String configFileLocation, String hbaseRoot) throws IOException {
        super(configFileLocation);
        this.hbaseRoot = hbaseRoot;
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
        KafkaHBaseTopology tridentKafkaTopology;
        if (args.length >= 1 && args[0].trim().equalsIgnoreCase(REMOTE)) {
            runLocally = false;
        }
        if (args.length >= 2 && args[1].length() > 0) {
            hbaseRoot = args[1].trim();
            tridentKafkaTopology
                    = new KafkaHBaseTopology(configFileLocation, hbaseRoot);
        } else {
            tridentKafkaTopology
                    = new KafkaHBaseTopology(configFileLocation);
        }

        tridentKafkaTopology.buildAndSubmit(TOPOLOGY_NAME, runLocally);
    }

    @Override
    protected Config buildConfig() {
        //set config.
        Config conf = new Config();
        conf.setMaxSpoutPending(2);
        HashMap<String, Object> hbaseConfig = getHBaseConfig();
        conf.put(HBASE_CONFIG, hbaseConfig);
        LOG.info("Build config");
        return conf;
    }

    private HashMap<String, Object> getHBaseConfig() {
        HashMap<String, Object> hbaseConfig = new HashMap<String, Object>();
        hbaseConfig.put("hbase.rootdir", hbaseRoot);
        hbaseConfig.put("hbase.zookeeper.quorum", "hostgroupmaster1-3-lloyds-20150923072909.node.dc1.consul," +
                "hostgroupmaster3-4-lloyds-20150923072909.node.dc1.consul," +
                "hostgroupmaster2-2-lloyds-20150923072909.node.dc1.consul");
        hbaseConfig.put("zookeeper.znode.parent", "/hbase-unsecure");
        return hbaseConfig;
    }

    @Override
    protected StormTopology buildTopology() {
        Scheme exploreScheme = new ExploreScheme();
        Fields fields = exploreScheme.getOutputFields();
        TridentHBaseMapper tridentHBaseMapper = new SimpleTridentHBaseMapper()
                .withColumnFamily("message data")
                .withColumnFields(fields)
                .withRowKeyField("ipAddress");

        HBaseValueMapper rowToStormValueMapper = new ExploreMessageValueMapper();

        HBaseState.Options options = new HBaseState.Options()
                .withConfigKey(HBASE_CONFIG)
                .withDurability(Durability.SYNC_WAL)
                .withMapper(tridentHBaseMapper)
                .withRowToStormValueMapper(rowToStormValueMapper)
                .withTableName("test_message");
        LOG.info("Created options");


        TridentTopology topology = new TridentTopology();
        OpaqueTridentKafkaSpout kafkaSpout = buildKafkaSpout(exploreScheme);
        StateFactory hbaseStateFactory = new HBaseStateFactory(options);
        LOG.info("Created HBaseStateFactory");
        Stream tridentStream = topology.newStream(TRIDENT_KAFKA_MESSAGE, kafkaSpout)
                .each(kafkaSpout.getOutputFields(), new ExploreLogFilter(this.getClass().getName()));
        LOG.info("Created stream and initialised with LogFilter.");
        tridentStream.partitionPersist(
                hbaseStateFactory, kafkaSpout.getOutputFields(), new HBaseUpdater(), new Fields()
        );
        LOG.info("Set up partition persist for HBase updater.");
        LOG.info("Calling build on topology");
        return topology.build();
    }

    private OpaqueTridentKafkaSpout buildKafkaSpout(Scheme scheme) {
        LOG.info("Build Kafka Spout");
        BrokerHosts zk = new ZkHosts(topologyConfig.getProperty(KAFKA_ZOOKEEPER_HOST_PORT));
        TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, topologyConfig.getProperty(KAFKA_TOPIC), TRIDENT_KAFKA_SPOUT);
        spoutConf.scheme = new SchemeAsMultiScheme(scheme);
        return new OpaqueTridentKafkaSpout(spoutConf);
    }

}
