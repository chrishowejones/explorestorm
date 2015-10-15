package com.devcycle.explorestorm.topologies;

import backtype.storm.Config;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.Scheme;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import com.devcycle.explorestorm.function.PrintFunction;
import com.devcycle.explorestorm.mapper.ExploreMessageValueMapper;
import com.devcycle.explorestorm.scheme.ExploreScheme;
import com.devcycle.explorestorm.scheme.LookupScheme;
import com.devcycle.explorestorm.util.HBaseConfigBuilder;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.storm.hbase.bolt.mapper.HBaseProjectionCriteria;
import org.apache.storm.hbase.bolt.mapper.HBaseValueMapper;
import org.apache.storm.hbase.trident.mapper.SimpleTridentHBaseMapper;
import org.apache.storm.hbase.trident.mapper.TridentHBaseMapper;
import org.apache.storm.hbase.trident.state.HBaseQuery;
import org.apache.storm.hbase.trident.state.HBaseState;
import org.apache.storm.hbase.trident.state.HBaseStateFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.BrokerHosts;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.state.StateFactory;

import java.io.IOException;
import java.util.HashMap;

/**
 * Created by chrishowe-jones on 14/10/15.
 */
public class ReadHBaseTopology extends BaseExploreTopology {

    public static final String COLUMN_FAMILY = "message data";
    public static final String ROW_KEY_FIELD = "ipAddress";
    public static final String TABLE_NAME = "test_message";

    public static final String EXPLORE_TOPOLOGY_PROPERTIES = "explore_topology.properties";
    public static final String REMOTE = "remote";
    public static final String KAFKA_ZOOKEEPER_HOST_PORT = "kafka.zookeeper.host.port";


    private static final Logger LOG = LoggerFactory.getLogger(ReadHBaseTopology.class);

    private static final String HBASE_CONFIG = "hbase.config";
    private static final String TOPOLOGY_NAME = "readHBaseTopology";
    private static final String HBASE_ROOT = "hbase.rootdir";
    private static final String KAFKA_TOPIC = "lookup.topic";
    private static final String LOOKUP_STREAM = "lookupStream";
    private static final String TRIDENT_KAFKA_SPOUT = "lookupSpout";
    private HBaseConfigBuilder hbaseConfigBuilder;

    public ReadHBaseTopology(String configFileLocation) throws IOException {
        super(configFileLocation);
    }

    public ReadHBaseTopology(String configFileLocation, String hbaseRoot) throws IOException {
        super(configFileLocation);
        topologyConfig.setProperty(HBASE_ROOT, hbaseRoot);
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
        ReadHBaseTopology readHBaseTopology;
        if (args.length >= 1 && args[0].trim().equalsIgnoreCase(REMOTE)) {
            runLocally = false;
        }
        if (args.length >= 2 && args[1].length() > 0) {
            hbaseRoot = args[1].trim();
            readHBaseTopology
                    = new ReadHBaseTopology(configFileLocation, hbaseRoot);
        } else {
            readHBaseTopology
                    = new ReadHBaseTopology(configFileLocation);
        }

        readHBaseTopology.buildAndSubmit(TOPOLOGY_NAME, runLocally);
    }

    @Override
    protected Config buildConfig() {
        //set config.red
        Config conf = new Config();
        conf.setMaxSpoutPending(2);
        HashMap<String, Object> hbaseConfig = getHBaseConfig();
        conf.put(HBASE_CONFIG, hbaseConfig);
        LOG.info("Build config");
        return conf;
    }

    @Override
    protected StormTopology buildTopology() {
        Scheme lookupScheme = new LookupScheme();
        Fields fields = lookupScheme.getOutputFields();
        TridentHBaseMapper tridentHBaseMapper = new SimpleTridentHBaseMapper()
                .withColumnFamily(COLUMN_FAMILY)
                .withColumnFields(fields)
                .withRowKeyField(ROW_KEY_FIELD);

        HBaseValueMapper rowToStormValueMapper = new ExploreMessageValueMapper();

        HBaseProjectionCriteria projectionCriteria = new HBaseProjectionCriteria();
        projectionCriteria.addColumn(new HBaseProjectionCriteria.ColumnMetaData("cf", "count"));

        HBaseState.Options options = new HBaseState.Options()
                .withConfigKey(HBASE_CONFIG)
                .withMapper(tridentHBaseMapper)
                .withRowToStormValueMapper(rowToStormValueMapper)
                .withTableName(TABLE_NAME);

        StateFactory factory = new HBaseStateFactory(options);

        TridentTopology topology = new TridentTopology();
        OpaqueTridentKafkaSpout kafkaSpout = buildKafkaSpout(lookupScheme);

        TridentState state = topology.newStaticState(factory);

        Stream stream = topology.newStream(LOOKUP_STREAM, kafkaSpout);
        stream = stream.stateQuery(state, new Fields(ROW_KEY_FIELD), new HBaseQuery(), new Fields("columnName","columnValue"));
        stream.each(new Fields(ROW_KEY_FIELD,"columnValue"), new PrintFunction(), new Fields());
        return topology.build();
    }

    private OpaqueTridentKafkaSpout buildKafkaSpout(Scheme lookupScheme) {
        LOG.info("Build Kafka Spout");
        BrokerHosts zk = new ZkHosts(topologyConfig.getProperty(KAFKA_ZOOKEEPER_HOST_PORT));
        TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, topologyConfig.getProperty(KAFKA_TOPIC), TRIDENT_KAFKA_SPOUT);
        spoutConf.scheme = new SchemeAsMultiScheme(lookupScheme);
        return new OpaqueTridentKafkaSpout(spoutConf);
    }


    HashMap<String, Object> getHBaseConfig() {
        return getHBaseConfigBuilder().getHBaseConfig(topologyConfig);
    }

    HBaseConfigBuilder getHBaseConfigBuilder() {
        if (hbaseConfigBuilder == null)
            this.hbaseConfigBuilder = new HBaseConfigBuilder();
        return hbaseConfigBuilder;
    }

    void setHBaseConfigBuilder(HBaseConfigBuilder hbaseConfigBuilder) {
        this.hbaseConfigBuilder = hbaseConfigBuilder;
    }
}
