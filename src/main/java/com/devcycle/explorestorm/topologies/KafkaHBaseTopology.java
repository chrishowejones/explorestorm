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
import org.apache.storm.hbase.bolt.mapper.HBaseValueMapper;
import org.apache.storm.hbase.trident.mapper.SimpleTridentHBaseMapper;
import org.apache.storm.hbase.trident.mapper.TridentHBaseMapper;
import org.apache.storm.hbase.trident.state.HBaseState;
import org.apache.storm.hbase.trident.state.HBaseStateFactory;
import org.apache.storm.hbase.trident.state.HBaseUpdater;
import storm.kafka.BrokerHosts;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.state.StateFactory;

import java.io.IOException;

/**
 * Topology to consume data from Kafka spout and persist in HBase.
 * <p/>
 * Created by chris howe-jones on 05/10/15.
 */
public class KafkaHBaseTopology extends BaseExploreTopology {

    private static final String TOPOLOGY_NAME = "kafkaHBaseTopology";
    public static final String KAFKA_ZOOKEEPER_HOST_PORT = "kafka.zookeeper.host.port";
    public static final String KAFKA_TOPIC = "kafka.topic";
    public static final String TRIDENT_KAFKA_SPOUT = "tridentKafkaSpout";
    public static final String TRIDENT_KAFKA_MESSAGE = "tridentKafkaMessage";

    public static final String KAFKA_PUBLISH_TOPIC = "kafka.publish.topic";
    public static final String EXPLORE_TOPOLOGY_PROPERTIES = "explore_topology.properties";
    public static final String REMOTE = "remote";
    public static final String HBASE_ROOT = "hbase.root";
    private final String hbaseRoot;


    public KafkaHBaseTopology(String configFileLocation) throws IOException {
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
        if (args.length >= 1 && args[0].equalsIgnoreCase(REMOTE)) {
            runLocally = false;
        }
        KafkaHBaseTopology tridentKafkaTopology
                = new KafkaHBaseTopology(configFileLocation);
        tridentKafkaTopology.buildAndSubmit(TOPOLOGY_NAME, runLocally);
    }

    @Override
    protected Config buildConfig() {
        //set config.
        Config conf = new Config();
        return conf;
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
                .withConfigKey(hbaseRoot)
                .withDurability(Durability.SYNC_WAL)
                .withMapper(tridentHBaseMapper)
                .withRowToStormValueMapper(rowToStormValueMapper)
                .withTableName("test_message");



        TridentTopology topology = new TridentTopology();
        OpaqueTridentKafkaSpout kafkaSpout = buildKafkaSpout(exploreScheme);
        StateFactory hbaseStateFactory = new HBaseStateFactory(options);
        Stream tridentStream = topology.newStream(TRIDENT_KAFKA_MESSAGE, kafkaSpout)
                .each(kafkaSpout.getOutputFields(), new ExploreLogFilter());
        tridentStream.partitionPersist(
                hbaseStateFactory, kafkaSpout.getOutputFields(), new HBaseUpdater(), new Fields()
        );

        return topology.build();
    }

    private OpaqueTridentKafkaSpout buildKafkaSpout(Scheme scheme) {
        BrokerHosts zk = new ZkHosts(topologyConfig.getProperty(KAFKA_ZOOKEEPER_HOST_PORT));
        TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, topologyConfig.getProperty(KAFKA_TOPIC), TRIDENT_KAFKA_SPOUT);
        spoutConf.scheme = new SchemeAsMultiScheme(scheme);
        return new OpaqueTridentKafkaSpout(spoutConf);
    }

}
