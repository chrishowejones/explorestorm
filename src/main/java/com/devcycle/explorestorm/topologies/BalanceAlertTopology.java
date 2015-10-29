package com.devcycle.explorestorm.topologies;

import backtype.storm.Config;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.Scheme;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import com.devcycle.explorestorm.function.ParseCBSMessage;
import com.devcycle.explorestorm.function.PrintFunction;
import com.devcycle.explorestorm.mapper.ExploreMessageValueMapper;
import com.devcycle.explorestorm.mapper.OCISRowToValueMapper;
import com.devcycle.explorestorm.scheme.CBSKafkaScheme;
import com.devcycle.explorestorm.util.HBaseConfigBuilder;
import org.apache.storm.hbase.bolt.mapper.HBaseProjectionCriteria;
import org.apache.storm.hbase.bolt.mapper.HBaseValueMapper;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
import org.apache.storm.hbase.trident.mapper.SimpleTridentHBaseMapper;
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
import java.util.Properties;

import static com.devcycle.explorestorm.topologies.HBaseConfig.HBASE_CONFIG;
import static com.devcycle.explorestorm.topologies.KafkaConfig.KAFKA_ZOOKEEPER_HOST_PORT;
import static com.devcycle.explorestorm.util.StormRunner.REMOTE;

/**
 * Created by chris howe-jones on 29/10/15.
 */
public class BalanceAlertTopology extends BaseExploreTopology {

    public static final String STREAM_NAME = "CbsBalanceAlertStream";
    public static final String EXPLORE_TOPOLOGY_PROPERTIES = "cbs_topology.properties";

    private static final String KAFKA_TOPIC = "CBSTopic";
    private static final String TRIDENT_KAFKA_SPOUT = "BalanceAlertSpout";
    private static final String TOPOLOGY_NAME = "balanceAlertTopology";
    private static final Logger LOG = LoggerFactory.getLogger(BalanceAlertTopology.class);
    private static final String TABLE_NAME = "OCISDetails";
    private HBaseConfigBuilder hbaseConfigBuilder;
    private Scheme cbsKafkaScheme;

    public BalanceAlertTopology(String configFileLocation) throws IOException {
        super(configFileLocation);
    }

    public BalanceAlertTopology(Properties configProperties) {
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
        BalanceAlertTopology balanceAlertTopology;
        if (args.length >= 1 && args[0].trim().equalsIgnoreCase(REMOTE)) {
            runLocally = false;
        }
        balanceAlertTopology
                = new BalanceAlertTopology(configFileLocation);

        balanceAlertTopology.buildAndSubmit(TOPOLOGY_NAME, runLocally);
    }

    /**
     * Used for DI when testing.
     *
     * @param cbsKafkaScheme
     */
    void setCbsKafkaScheme(CBSKafkaScheme cbsKafkaScheme) {
        this.cbsKafkaScheme = cbsKafkaScheme;
    }

    /**
     * Return the CBSKafkaScheme or create one if null.
     *
     * @return scheme
     */
     Scheme getCBSKafkaScheme() {
         if (cbsKafkaScheme == null)
             new CBSKafkaScheme();
        return cbsKafkaScheme;
    }

    /**
     * Construct and return the config for this topology.
     *
     * @return config for this topology.
     */
    @Override
    protected Config buildConfig() {
        final Config config = new Config();
        config.put(HBASE_CONFIG.toString(), buildHBaseConfig());

        return config;
    }

    /**
     * Build the balance alert topology.
     *
     * @return topology
     */
    @Override
    protected StormTopology buildTopology() {
        TridentTopology topology = buildTridentTopology();
        return topology.build();
    }

    /**
     * Build Trident Topology
     *
     * @return trident topology
     */
    TridentTopology buildTridentTopology() {
        TridentTopology topology = new TridentTopology();

        // set up kafka spout
        Scheme cbsKafkaScheme = getCBSKafkaScheme();

        OpaqueTridentKafkaSpout kafkaSpout = buildKafkaSpout(cbsKafkaScheme);

        SimpleTridentHBaseMapper mapper = new SimpleTridentHBaseMapper().withRowKeyField(ParseCBSMessage.FIELD_T_IPPSTEM);
        HBaseProjectionCriteria projectionCriteria = new HBaseProjectionCriteria();
        projectionCriteria.addColumn(new HBaseProjectionCriteria.ColumnMetaData("cf_threshold", "threshold"));
        HBaseValueMapper rowToStormValueMapper = new ExploreMessageValueMapper();

        HBaseState.Options options = new HBaseState.Options()
                .withConfigKey(HBASE_CONFIG.toString())
                .withMapper(mapper)
                .withProjectionCriteria(projectionCriteria)
                .withRowToStormValueMapper(rowToStormValueMapper)
                .withTableName(TABLE_NAME);


        Stream stream = topology.newStream(STREAM_NAME, kafkaSpout)
            .each(kafkaSpout.getOutputFields(), new ParseCBSMessage(CBSKafkaScheme.FIELD_JSON_MESSAGE), ParseCBSMessage.getEmittedFields());

        StateFactory factory = new HBaseStateFactory(options);

        TridentState state = topology.newStaticState(factory);

        stream.stateQuery(state, new Fields(ParseCBSMessage.FIELD_T_IPPSTEM), new HBaseQuery(), new Fields("columnName", "columnValue"))
            .each(new Fields(ParseCBSMessage.FIELD_T_IPPSTEM, "columnName", "columnValue"), new PrintFunction(), new Fields());

        return topology;
    }

    /**
     * Get the HBaseConfigBuilder.
     *
     * @return HBaseConfigBuilder
     */
    HBaseConfigBuilder getHBaseConfigBuilder() {
        if (hbaseConfigBuilder == null)
            this.hbaseConfigBuilder = new HBaseConfigBuilder();
        return hbaseConfigBuilder;
    }

    /**
     * Set HBaseConfigBuilder to be used for dependency injection and testing.
     *
     * @param hbaseConfigBuilder
     */
    void setHBaseConfigBuilder(HBaseConfigBuilder hbaseConfigBuilder) {
        this.hbaseConfigBuilder = hbaseConfigBuilder;
    }

    private HashMap<String, Object> buildHBaseConfig() {
        return getHBaseConfigBuilder().getHBaseConfig(topologyConfig);
    }

    private OpaqueTridentKafkaSpout buildKafkaSpout(Scheme cbsKafkaScheme) {
        LOG.info("Build Kafka Spout");
        BrokerHosts zk = new ZkHosts(topologyConfig.getProperty(KAFKA_ZOOKEEPER_HOST_PORT.toString()));
        TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, topologyConfig.getProperty(KAFKA_TOPIC), TRIDENT_KAFKA_SPOUT);
        spoutConf.scheme = new SchemeAsMultiScheme(cbsKafkaScheme);
        return new OpaqueTridentKafkaSpout(spoutConf);
    }

}
