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
import com.devcycle.explorestorm.function.AddTimestampFunction;
import com.devcycle.explorestorm.function.CreateAccountTxnRowKey;
import com.devcycle.explorestorm.function.ParseCBSMessage;
import com.devcycle.explorestorm.mapper.AccountTransactionMapper;
import com.devcycle.explorestorm.scheme.CBSKafkaScheme;
import com.devcycle.explorestorm.scheme.CBSMessageFields;
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


    public static final String STREAM_NAME = "TestHBaseStream";
    public static final String EXPLORE_TOPOLOGY_PROPERTIES = "cbs_topology.properties";
    public static final String LOCAL_EXPLORE_TOPOLOGY_PROPERTIES = "local_cbs_topology.properties";

    private static final Logger LOG = LoggerFactory.getLogger(PersistCBSTopology.class);
    private static final String KAFKA_TOPIC = "CBSTopic";
    private static final String TRIDENT_KAFKA_SPOUT = "TestMessageSpout";
    private static final String TOPOLOGY_NAME = "testTimestamp";
    private static final String STATEMENT_DATA_CF = "s";
    private static final String MESSAGE_CF = "m";
    private static final String ROW_KEY_FIELD = "account-txn-date";
    private static final String TABLE_NAME = "account-txns";
    private static final List<String> FIELDS_TO_PARSE = new ArrayList<String>() {
        {
            add(CBSMessageFields.FIELD_SEQNUM);
            add(CBSMessageFields.FIELD_TIME);
            add(CBSMessageFields.FIELD_ACCOUNT_NUMBER);
            add(CBSMessageFields.FIELD_TXN_TYPE);
            add(CBSMessageFields.FIELD_TXN_CODE);
            add(CBSMessageFields.FIELD_TXN_AMOUNT);
            add(CBSMessageFields.FIELD_CURRENCY_CDE);
            add(CBSMessageFields.FIELD_CURRENT_ACCOUNT_BALANCE);
            add(CBSMessageFields.FIELD_CURRENT_DATE);
            add(CBSMessageFields.FIELD_TXN_DATE);
            add(CBSMessageFields.FIELD_TXN_NARRATIVE);
            add(CBSMessageFields.FIELD_MSG_TIMESTAMP);
            add(CBSMessageFields.FIELD_FULL_MESSAGE);
        }
    };
    private HBaseConfigBuilder hbaseConfigBuilder;
    private Scheme cbsKafkaScheme;

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
        PersistCBSTopology persistCBSTopology;
        if (args.length >= 1 && args[0].trim().equalsIgnoreCase(REMOTE)) {
            runLocally = false;
        } else {
            configFileLocation = LOCAL_EXPLORE_TOPOLOGY_PROPERTIES;
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
        String numberOfWorkers = topologyConfig.getProperty("numberOfWorkers");
        if (numberOfWorkers != null && numberOfWorkers.length() > 0)
            config.setNumWorkers(Integer.parseInt(numberOfWorkers));
        return config;
    }

    /**
     * Build the topology for a Trident Stream that consumes a CBS message from Kafka and persists it in HBase keyed by account/txn-date
     *
     * @return
     */
    @Override
    protected StormTopology buildTopology() {
        return buildTridentTopology().build();
    }

    /**
     * Build Trident Topology for a Trident Stream that consumes a CBS message from Kafka and persists it in HBase keyed by account/txn-date
     *
     * @return
     */
    TridentTopology buildTridentTopology() {
        TridentTopology topology = new TridentTopology();
        Scheme cbsKafkaScheme = getCBSKafkaScheme();

        OpaqueTridentKafkaSpout kafkaSpout = buildKafkaSpout(cbsKafkaScheme);

        // Create trident stream to read, parse and transform CBS message ready for persisting
        List<String> parsedFields = new ArrayList<>(FIELDS_TO_PARSE);
        parsedFields.add(0, CBSKafkaScheme.FIELD_JSON_MESSAGE);
        parsedFields.add(1, CBSMessageFields.FIELD_MSG_TIMESTAMP_STORM);
        Fields outputFieldsFromParse = new Fields(parsedFields);


        String parallelismHint = topologyConfig.getProperty("parallelismHint");
        Integer hint = null;
        if (parallelismHint != null && parallelismHint.length() > 0)
            hint = Integer.parseInt(parallelismHint);
        Stream stream = topology.newStream(STREAM_NAME, kafkaSpout);
        if (hint != null)
            stream = stream.parallelismHint(hint);
        stream = stream.each(kafkaSpout.getOutputFields(), new AddTimestampFunction(), new Fields(CBSMessageFields.FIELD_MSG_TIMESTAMP_STORM))
                .each(new Fields(CBSKafkaScheme.FIELD_JSON_MESSAGE, CBSMessageFields.FIELD_MSG_TIMESTAMP_STORM), new ParseCBSMessage(CBSKafkaScheme.FIELD_JSON_MESSAGE, FIELDS_TO_PARSE), new Fields(FIELDS_TO_PARSE))
                .each(outputFieldsFromParse,
                        new RemoveInvalidMessages(CBSMessageFields.FIELD_SEQNUM,
                                new String[]{CBSMessageFields.FIELD_ACCOUNT_NUMBER, CBSMessageFields.FIELD_TXN_DATE}))
                .each(new Fields(FIELDS_TO_PARSE), new CreateAccountTxnRowKey(), new Fields(ROW_KEY_FIELD));

        // set up HBase state factory
        List<String> transformedFieldsList = new ArrayList<String>(parsedFields);
        transformedFieldsList.add(ROW_KEY_FIELD);
        Fields transformedFields = new Fields(transformedFieldsList);

        List<String> fieldsToPersist = getFieldsToPersistForAccountTxn();

        List<String> afterHBaseTimestamp = new ArrayList<>();
        afterHBaseTimestamp.addAll(transformedFieldsList);
        afterHBaseTimestamp.add(CBSMessageFields.FIELD_MSG_TIMESTAMP_STORMHBASE);

        Fields afterHBaseTimestampFields = new Fields(afterHBaseTimestamp);
        stream = stream.each(transformedFields, new AddTimestampFunction(), new Fields(CBSMessageFields.FIELD_MSG_TIMESTAMP_STORMHBASE))
                .each(afterHBaseTimestampFields, new ExploreLogFilter(this.getClass().getName()));


        HBaseStateFactory factory = buildCBSHBaseStateFactory(fieldsToPersist);
        stream.partitionPersist(factory, transformedFields, new HBaseUpdater());
        return topology;
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
        messageFields.add(CBSMessageFields.FIELD_FULL_MESSAGE);

        TridentHBaseMapper tridentHBaseMapper = new AccountTransactionMapper()
                .withRowKeyField(ROW_KEY_FIELD)
                .withColumnFamilies(columnFamilies)
                .withTransactionId(CBSMessageFields.FIELD_SEQNUM)
                .withColumnFieldPrefixes(STATEMENT_DATA_CF, fieldsToPersist)
                .withColumnFieldPrefixes(MESSAGE_CF, messageFields);

        HBaseState.Options options = new HBaseState.Options()
                .withConfigKey(HBASE_CONFIG.toString())
                .withDurability(Durability.SYNC_WAL)
                .withMapper(tridentHBaseMapper)
                .withTableName(TABLE_NAME);

        return new HBaseStateFactory(options);
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

    private List<String> getFieldsToPersistForAccountTxn() {
        List<String> fieldsToPersist = new ArrayList<>();
        fieldsToPersist.add(CBSMessageFields.FIELD_SEQNUM);
        fieldsToPersist.add(CBSMessageFields.FIELD_TIME);
        fieldsToPersist.add(CBSMessageFields.FIELD_TXN_TYPE);
        fieldsToPersist.add(CBSMessageFields.FIELD_TXN_CODE);
        fieldsToPersist.add(CBSMessageFields.FIELD_TXN_AMOUNT);
        fieldsToPersist.add(CBSMessageFields.FIELD_CURRENCY_CDE);
        fieldsToPersist.add(CBSMessageFields.FIELD_CURRENT_ACCOUNT_BALANCE);
        fieldsToPersist.add(CBSMessageFields.FIELD_CURRENT_DATE);
        fieldsToPersist.add(CBSMessageFields.FIELD_TXN_DATE);
        fieldsToPersist.add(CBSMessageFields.FIELD_TXN_NARRATIVE);
        fieldsToPersist.add(CBSMessageFields.FIELD_MSG_TIMESTAMP);
        fieldsToPersist.add(CBSMessageFields.FIELD_MSG_TIMESTAMP_STORM);
        return fieldsToPersist;
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
