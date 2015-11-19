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
import com.devcycle.explorestorm.function.CreateOCISAccountRowKey;
import com.devcycle.explorestorm.function.ParseCBSMessage;
import com.devcycle.explorestorm.function.RaiseLowBalanceAlert;
import com.devcycle.explorestorm.mapper.OCISRowToValueMapper;
import com.devcycle.explorestorm.scheme.CBSKafkaScheme;
import com.devcycle.explorestorm.scheme.CBSMessageFields;
import com.devcycle.explorestorm.util.HBaseConfigBuilder;
import org.apache.storm.hbase.bolt.mapper.HBaseProjectionCriteria;
import org.apache.storm.hbase.bolt.mapper.HBaseValueMapper;
import org.apache.storm.hbase.trident.mapper.SimpleTridentHBaseMapper;
import org.apache.storm.hbase.trident.state.HBaseQuery;
import org.apache.storm.hbase.trident.state.HBaseState;
import org.apache.storm.hbase.trident.state.HBaseStateFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.BrokerHosts;
import storm.kafka.ZkHosts;
import storm.kafka.trident.*;
import storm.kafka.trident.mapper.FieldNameBasedTupleToKafkaMapper;
import storm.kafka.trident.selector.DefaultTopicSelector;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.state.StateFactory;

import java.io.IOException;
import java.util.*;

import static com.devcycle.explorestorm.topologies.HBaseConfig.HBASE_CONFIG;
import static com.devcycle.explorestorm.topologies.KafkaConfig.KAFKA_ZOOKEEPER_HOST_PORT;
import static com.devcycle.explorestorm.util.StormRunner.REMOTE;

/**
 * Created by chris howe-jones on 29/10/15.
 */
public class BalanceAlertTopology extends BaseExploreTopology {

    private static final String STREAM_NAME = "CbsBalanceAlertStream";
    private static final String EXPLORE_TOPOLOGY_PROPERTIES = "cbs_topology.properties";
    public static final String METADATA_BROKER_LIST = "metadata.broker.list";
    public static final String REQUEST_REQUIRED_ACKS = "request.required.acks";

    private static final Logger LOG = LoggerFactory.getLogger(BalanceAlertTopology.class);

    private static final String KAFKA_PUBLISH_TOPIC = "kafka.publish.topic";
    private static final String ROW_KEY = "rowKey";
    private static final String KAFKA_TOPIC = "CBSTopic";
    private static final String TRIDENT_KAFKA_SPOUT = "BalanceAlertSpout";
    private static final String TOPOLOGY_NAME = "balanceAlertTopology";
    private static final String TABLE_NAME = "OCISDetails";
    private static final String LOW_BALANCE_ALERT = "lowBalanceAlert";
    private static final String SERIALIZER_CLASS = "serializer.class";
    private static final String KAFKA_SERIALIZER_STRING_ENCODER = "kafka.serializer.StringEncoder";
    private static final String ACCOUNT_NUMBER_STRING = "accountNumberStr";

    private static final List<String> FIELDS_TO_PARSE = new ArrayList<String>() {
        {
            add(CBSMessageFields.FIELD_SEQNUM);
            add(CBSMessageFields.FIELD_ACCOUNT_NUMBER);
            add(CBSMessageFields.FIELD_TXN_TYPE);
            add(CBSMessageFields.FIELD_TXN_CLASS);
            add(CBSMessageFields.FIELD_CURRENT_ACCOUNT_BALANCE);
            add(CBSMessageFields.FIELD_TXN_AMOUNT);
            add(CBSMessageFields.FIELD_MSG_TIMESTAMP);
        }
    };

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
        BalanceAlertTopology balanceAlertTopology;
        if (args.length >= 1 && args[0].trim().equalsIgnoreCase(REMOTE)) {
            runLocally = false;
        }
        balanceAlertTopology
                = new BalanceAlertTopology(configFileLocation);

        balanceAlertTopology.buildAndSubmit(TOPOLOGY_NAME, runLocally);
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
        Properties props = new Properties();
        props.put(METADATA_BROKER_LIST, topologyConfig.getProperty(METADATA_BROKER_LIST));
        props.put(REQUEST_REQUIRED_ACKS, topologyConfig.getProperty(REQUEST_REQUIRED_ACKS));
        props.put(SERIALIZER_CLASS, KAFKA_SERIALIZER_STRING_ENCODER);
        config.put(TridentKafkaState.KAFKA_BROKER_PROPERTIES, props);
        String numberOfWorkers = topologyConfig.getProperty("numberOfWorkers");
        if (numberOfWorkers != null && numberOfWorkers.length() > 0)
            config.setNumWorkers(Integer.parseInt(numberOfWorkers));
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
    private Scheme getCBSKafkaScheme() {
        if (cbsKafkaScheme == null)
            cbsKafkaScheme = new CBSKafkaScheme();
        return cbsKafkaScheme;
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

        final Fields columnFields = new Fields(OCISDetails.THRESHOLD);
        HBaseState.Options options = buildOptions(columnFields);

        String parallelismHint = topologyConfig.getProperty("parallelismHint");
        Integer hint = null;
        if (parallelismHint != null && parallelismHint.length() > 0)
            hint = Integer.parseInt(parallelismHint);
        Stream stream = topology.newStream(STREAM_NAME, kafkaSpout);
        if (hint != null)
            stream = stream.parallelismHint(hint);
        stream = stream.each(kafkaSpout.getOutputFields(),
                new ParseCBSMessage(CBSKafkaScheme.FIELD_JSON_MESSAGE, FIELDS_TO_PARSE), new Fields(FIELDS_TO_PARSE))
                .each(new Fields(CBSMessageFields.FIELD_SEQNUM, CBSMessageFields.FIELD_ACCOUNT_NUMBER, CBSMessageFields.FIELD_CURRENT_ACCOUNT_BALANCE, CBSMessageFields.FIELD_TXN_AMOUNT,
                                CBSMessageFields.FIELD_TXN_CLASS, CBSMessageFields.FIELD_TXN_TYPE),
                        new FilterNull())
                .each(new Fields(CBSMessageFields.FIELD_ACCOUNT_NUMBER), new CreateOCISAccountRowKey(), new Fields(ROW_KEY));

        StateFactory factory = new HBaseStateFactory(options);

        TridentState state = topology.newStaticState(factory);

        List<String> outputFields = buildOutputFields(columnFields);

        stream = stream.stateQuery(state, new Fields(ROW_KEY), new HBaseQuery(), columnFields)
                .each(new Fields(OCISDetails.THRESHOLD), new FilterNull())
                .each(new Fields(CBSMessageFields.FIELD_CURRENT_ACCOUNT_BALANCE, CBSMessageFields.FIELD_ACCOUNT_NUMBER, OCISDetails.THRESHOLD, CBSMessageFields.FIELD_TXN_AMOUNT,
                                CBSMessageFields.FIELD_TXN_CLASS, CBSMessageFields.FIELD_TXN_TYPE, CBSMessageFields.FIELD_MSG_TIMESTAMP),
                        new RaiseLowBalanceAlert(), new Fields(ACCOUNT_NUMBER_STRING, LOW_BALANCE_ALERT))
                .each(new Fields(ACCOUNT_NUMBER_STRING, LOW_BALANCE_ALERT), new FilterNull());

        buildKafkaSink(stream);

        return topology;
    }

    /**
     * Get the HBaseConfigBuilder.
     *
     * @return HBaseConfigBuilder
     */
    private HBaseConfigBuilder getHBaseConfigBuilder() {
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

    private void buildKafkaSink(Stream stream) {
        TridentKafkaStateFactory stateFactory = new TridentKafkaStateFactory()
                .withKafkaTopicSelector(new DefaultTopicSelector(topologyConfig.getProperty(KAFKA_PUBLISH_TOPIC)))
                .withTridentTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper(ACCOUNT_NUMBER_STRING, LOW_BALANCE_ALERT));
        stream.partitionPersist(stateFactory, new Fields(ACCOUNT_NUMBER_STRING, LOW_BALANCE_ALERT), new TridentKafkaUpdater(), new Fields());
    }

    private HBaseState.Options buildOptions(Fields columnFields) {
        SimpleTridentHBaseMapper mapper = new SimpleTridentHBaseMapper().withRowKeyField(ROW_KEY)
                .withColumnFamily(OCISDetails.CF_THRESHOLD)
                .withColumnFields(columnFields);
        HBaseProjectionCriteria projectionCriteria = new HBaseProjectionCriteria();
        projectionCriteria.addColumn(new HBaseProjectionCriteria.ColumnMetaData(OCISDetails.CF_THRESHOLD, OCISDetails.THRESHOLD));
        Map<String, Fields> fieldsRequired = new LinkedHashMap<>();
        fieldsRequired.put(OCISDetails.CF_THRESHOLD, columnFields);
        HBaseValueMapper rowToStormValueMapper = new OCISRowToValueMapper(fieldsRequired);

        return new HBaseState.Options()
                .withConfigKey(HBASE_CONFIG.toString())
                .withMapper(mapper)
                .withRowToStormValueMapper(rowToStormValueMapper)
                .withTableName(TABLE_NAME);
    }

    private List<String> buildOutputFields(Fields columnFields) {
        String[] fieldsArray =
                {ROW_KEY, CBSMessageFields.FIELD_CURRENT_ACCOUNT_BALANCE, CBSMessageFields.FIELD_TXN_AMOUNT, CBSMessageFields.FIELD_TXN_TYPE};
        return buildColumnFields(columnFields, fieldsArray);
    }

    private List<String> buildColumnFields(Fields columnFields, String[] fieldsArray) {
        List<String> outputFields = new ArrayList<>();
        outputFields.addAll(Arrays.asList(fieldsArray));
        for (String fieldName : columnFields.toList()) {
            outputFields.add(fieldName);
        }
        return outputFields;
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
