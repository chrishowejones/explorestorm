package com.devcycle.explorestorm.topologies;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.Scheme;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import com.devcycle.explorestorm.filter.ExploreLogFilter;
import com.devcycle.explorestorm.function.PrintFunction;
import com.devcycle.explorestorm.scheme.CBSKafkaScheme;
import com.devcycle.explorestorm.util.HBaseConfigBuilder;
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

/**
 * Topology to read CBS messages from kafka and persist them to HBase.
 * <p/>
 * Created by chrishowe-jones on 21/10/15.
 */
public class PersistCBSTopology extends BaseExploreTopology {


    public static final String STREAM_NAME = "CbsHBaseStream";
    private static final Logger LOG = LoggerFactory.getLogger(PersistCBSTopology.class);
    private static final java.lang.String KAFKA_TOPIC = "CBSTopic";
    private static final String TRIDENT_KAFKA_SPOUT = "CBSMessageSpout";
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
                .each(kafkaSpout.getOutputFields(), new PrintFunction(), new Fields());
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
