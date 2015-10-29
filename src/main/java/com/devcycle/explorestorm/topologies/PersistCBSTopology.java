package com.devcycle.explorestorm.topologies;

import backtype.storm.Config;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.Scheme;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import com.devcycle.explorestorm.filter.RemoveInvalidMessages;
import com.devcycle.explorestorm.function.CreateRowKey;
import com.devcycle.explorestorm.function.ParseCBSMessage;
import com.devcycle.explorestorm.function.PrintFunction;
import com.devcycle.explorestorm.mapper.AccountTransactionMapper;
import com.devcycle.explorestorm.scheme.CBSKafkaScheme;
import com.devcycle.explorestorm.util.HBaseConfigBuilder;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.storm.hbase.trident.mapper.TridentHBaseMapper;
import org.apache.storm.hbase.trident.state.HBaseState;
import org.apache.storm.hbase.trident.state.HBaseStateFactory;
import org.apache.storm.hbase.trident.state.HBaseUpdater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.BrokerHosts;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.Stream;
import storm.trident.TridentTopology;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
    public static final String EXPLORE_TOPOLOGY_PROPERTIES = "cbs_topology.properties";

    private static final Logger LOG = LoggerFactory.getLogger(PersistCBSTopology.class);
    private static final java.lang.String KAFKA_TOPIC = "CBSTopic";
    private static final String TRIDENT_KAFKA_SPOUT = "CBSMessageSpout";
    private static final String TOPOLOGY_NAME = "persistCBSTopology";
    private static final String STATEMENT_DATA_CF = "statement_data";
    private static final String MESSAGE_CF = "message_data";
    private static final String ROW_KEY_FIELD = "account-txn-date";
    private static final String TABLE_NAME = "account-txns";
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

    /**
     * Create PersistCBSTopology
     *
     * @param configProperties properties file to configure this topology.
     */
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

    /**
     * Returns the CBSKafka Scheme.
     *
     * @return Scheme for CBS Kafka messages.
     */
    public Scheme getCBSKafkaScheme() {
        if (cbsKafkaScheme == null)
            cbsKafkaScheme = new CBSKafkaScheme();
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
     * Build the topology for a Trident Stream that consumes a CBS message from Kafka and persists it in HBase keyed by account/txn-date
     * @return
     */
    @Override
    protected StormTopology buildTopology() {
        return buildTridentTopology().build();
    }

    /**
     * Build Trident Topology for a Trident Stream that consumes a CBS message from Kafka and persists it in HBase keyed by account/txn-date
     * @return
     */
    TridentTopology buildTridentTopology() {
        TridentTopology topology = new TridentTopology();
        Scheme cbsKafkaScheme = getCBSKafkaScheme();

        OpaqueTridentKafkaSpout kafkaSpout = buildKafkaSpout(cbsKafkaScheme);

        // Create trident stream to read, parse and transform CBS message ready for persisting
        final List<String> parsedFields = ParseCBSMessage.getEmittedFields().toList();
        parsedFields.add(0, CBSKafkaScheme.FIELD_JSON_MESSAGE);
        Fields outputFieldsFromParse = new Fields(parsedFields);


        final Stream stream = topology.newStream(STREAM_NAME, kafkaSpout)
                .each(kafkaSpout.getOutputFields(), new ParseCBSMessage(CBSKafkaScheme.FIELD_JSON_MESSAGE), ParseCBSMessage.getEmittedFields())
                .each(outputFieldsFromParse,
                        new RemoveInvalidMessages(ParseCBSMessage.FIELD_SEQNUM,
                                new String[]{ParseCBSMessage.FIELD_T_IPPSTEM, ParseCBSMessage.FIELD_T_IPTD}))
                .each(ParseCBSMessage.getEmittedFields(), new CreateRowKey(), new Fields(ROW_KEY_FIELD));

        // set up HBase state factory
        Fields transformedFields = new Fields(
                ParseCBSMessage.FIELD_SEQNUM,
                ParseCBSMessage.FIELD_T_IPTETIME,
                ParseCBSMessage.FIELD_T_IPPBR,
                ParseCBSMessage.FIELD_T_IPPSTEM,
                ParseCBSMessage.FIELD_T_IPTTST,
                ParseCBSMessage.FIELD_T_IPTCLCDE,
                ParseCBSMessage.FIELD_T_IPTAM,
                ParseCBSMessage.FIELD_T_IPCURCDE,
                ParseCBSMessage.FIELD_T_HIACBL,
                ParseCBSMessage.FIELD_T_IPCDATE,
                ParseCBSMessage.FIELD_T_IPTD,
                ParseCBSMessage.FIELD_T_IPTXNARR,
                ParseCBSMessage.FIELD_FULL_MESSAGE,
                ROW_KEY_FIELD
        );

        List<String> fieldsToPersist = getFieldsToPersistForAccountTxn();


        HBaseStateFactory factory = buildCBSHBaseStateFactory(fieldsToPersist);
        stream.partitionPersist(factory, transformedFields, new HBaseUpdater());
        return topology;
    }

    private List<String> getFieldsToPersistForAccountTxn() {
        List<String> fieldsToPersist = new ArrayList<>();
        fieldsToPersist.add(ParseCBSMessage.FIELD_SEQNUM);
        fieldsToPersist.add(ParseCBSMessage.FIELD_T_IPTETIME);
        fieldsToPersist.add(ParseCBSMessage.FIELD_T_IPTTST);
        fieldsToPersist.add(ParseCBSMessage.FIELD_T_IPTCLCDE);
        fieldsToPersist.add(ParseCBSMessage.FIELD_T_IPTAM);
        fieldsToPersist.add(ParseCBSMessage.FIELD_T_IPCURCDE);
        fieldsToPersist.add(ParseCBSMessage.FIELD_T_HIACBL);
        fieldsToPersist.add(ParseCBSMessage.FIELD_T_IPCDATE);
        fieldsToPersist.add(ParseCBSMessage.FIELD_T_IPTD);
        fieldsToPersist.add(ParseCBSMessage.FIELD_T_IPTXNARR);
        return fieldsToPersist;
    }

    /**
     * Build CBS HBase State factory for the CBS messages that will be queried by transactions per account per day (Statement queries)
     *
     * @param fieldsToPersist- fields to persist in HBase.
     * @return HBaseStateFactory
     */
    HBaseStateFactory buildCBSHBaseStateFactory(List<String> fieldsToPersist) {
        List<String> columnFamilies = new ArrayList<>();
        columnFamilies.add(STATEMENT_DATA_CF);
        columnFamilies.add(MESSAGE_CF);


        List<String> messageFields = new ArrayList<>();
        messageFields.add(ParseCBSMessage.FIELD_FULL_MESSAGE);

        TridentHBaseMapper tridentHBaseMapper = new AccountTransactionMapper()
                .withRowKeyField(ROW_KEY_FIELD)
                .withColumnFamilies(columnFamilies)
                .withTransactionId(ParseCBSMessage.FIELD_SEQNUM)
                .withColumnFieldPrefixes(STATEMENT_DATA_CF, fieldsToPersist)
                .withColumnFieldPrefixes(MESSAGE_CF, messageFields);

        HBaseState.Options options = new HBaseState.Options()
                .withConfigKey(HBASE_CONFIG.toString())
                .withDurability(Durability.SYNC_WAL)
                .withMapper(tridentHBaseMapper)
                .withTableName(TABLE_NAME);;
        HBaseStateFactory factory = new HBaseStateFactory(options);
        return factory;
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

    /**
     * Set CBSKafkaScheme to be used for dependency injection and testing.
     *
     * @param cbsKafkaScheme
     */
    void setCbsKafkaScheme(CBSKafkaScheme cbsKafkaScheme) {
        this.cbsKafkaScheme = cbsKafkaScheme;
    }

    /**
     * Set Kafka Spout used for testing.
     *
     * @param spout
     */
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
