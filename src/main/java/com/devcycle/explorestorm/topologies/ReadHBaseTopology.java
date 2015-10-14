package com.devcycle.explorestorm.topologies;

import backtype.storm.Config;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;

/**
 * Created by chrishowe-jones on 14/10/15.
 */
public class ReadHBaseTopology extends BaseExploreTopology {

    public static final String KAFKA_ZOOKEEPER_HOST_PORT = "kafka.zookeeper.host.port";
    public static final String KAFKA_TOPIC = "kafka.topic";
    public static final String TRIDENT_KAFKA_SPOUT = "hbaseKafkaSpout";
    public static final String TRIDENT_KAFKA_MESSAGE = "hbaseKafkaMessage";

    public static final String EXPLORE_TOPOLOGY_PROPERTIES = "explore_topology.properties";
    public static final String REMOTE = "remote";


    private Logger LOG = LoggerFactory.getLogger(KafkaHBaseTopology.class);

    private static final String HBASE_CONFIG = "hbase.config";
    private static final String TOPOLOGY_NAME = "readHBaseTopology";
    private static final java.lang.String HBASE_ROOT = "hbase.rootdir";
    private final String hbaseRoot;

    public ReadHBaseTopology(String configFileLocation) throws IOException {
        super(configFileLocation);
        this.hbaseRoot = topologyConfig.getProperty(HBASE_ROOT);
    }

    public ReadHBaseTopology(String configFileLocation, String hbaseRoot) throws IOException {
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

    @Override
    protected StormTopology buildTopology() {
        return null;
    }


    private HashMap<String,Object> getHBaseConfig() {
        HashMap<String, Object> hbaseConfig = new HashMap<String, Object>();
        hbaseConfig.put("hbase.rootdir", hbaseRoot);
        hbaseConfig.put("hbase.zookeeper.quorum", "hostgroupmaster1-3-lloyds-20150923072909.node.dc1.consul," +
                "hostgroupmaster3-4-lloyds-20150923072909.node.dc1.consul," +
                "hostgroupmaster2-2-lloyds-20150923072909.node.dc1.consul");
        hbaseConfig.put("zookeeper.znode.parent", "/hbase-unsecure");
        return hbaseConfig;
    }
}
