package com.devcycle.explorestorm.topologies;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.Scheme;
import com.devcycle.explorestorm.util.HBaseConfigBuilder;
import storm.kafka.trident.OpaqueTridentKafkaSpout;

import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;

import static com.devcycle.explorestorm.topologies.HBaseConfig.HBASE_CONFIG;

/**
 * Created by chris howe-jones on 29/10/15.
 */
public class BalanceAlertTopology extends BaseExploreTopology {

    public static final String STREAM_NAME = "CbsBalanceAlertStream";
    public static final String EXPLORE_TOPOLOGY_PROPERTIES = "cbs_topology.properties";

    private static final String KAFKA_TOPIC = "CBSTopic";
    private static final String TRIDENT_KAFKA_SPOUT = "BalanceAlertSpout";
    private static final String TOPOLOGY_NAME = "balanceAlertTopology";
    private HBaseConfigBuilder hbaseConfigBuilder;
    private Scheme cbsKafkaScheme;
    private OpaqueTridentKafkaSpout kafkaSpout;

    public BalanceAlertTopology(String configFileLocation) throws IOException {
        super(configFileLocation);
    }

    public BalanceAlertTopology(Properties configProperties) {
        super(configProperties);
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
     * @return  topology
     */
    @Override
    protected StormTopology buildTopology() {
        return null;
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
}
