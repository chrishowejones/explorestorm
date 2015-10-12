package com.devcycle.explorestorm.topologies;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import com.devcycle.explorestorm.util.StormRunner;
import org.apache.log4j.Logger;
import storm.trident.TridentTopology;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by chrishowe-jones on 17/09/15.
 */
public abstract class BaseExploreTopology {

    private static final Logger LOG = Logger.getLogger(BaseExploreTopology.class);
    public static final String LOCALCLUSTERHOST = "localhost";
    public static final long LOCALCLUSTERPORT = 2181L;
    public static final int RUNTIME_IN_SECONDS = 6000;


    protected Properties topologyConfig;

    /**
     * Create base class for topologies.
     *
     * @param configFileLocation
     * @throws IOException
     */
    public BaseExploreTopology(String configFileLocation) throws IOException {
        loadTopologyConfig(configFileLocation);
    }

    /**
     * Load the topology config properties.
     *
     * @param configFileLocation
     * @throws IOException
     */
    protected void loadTopologyConfig(String configFileLocation) throws IOException {
        topologyConfig = new Properties();
        topologyConfig.load(ClassLoader.getSystemResourceAsStream(configFileLocation));
    }

    /*
    Build and submit a TridentTopology that consumes messages from a Kafka topic, logs them and writes them to another Kafka topic.
     */
    protected void buildAndSubmit(String topologyName, boolean runLocally) throws InterruptedException, InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        // create a Trident Topology
        LOG.debug("build and submit");
        StormTopology topology = buildTopology();
        Config conf = buildConfig();

        if (runLocally) {
            runLocally(topology, topologyName, conf);
        } else {
            runRemotely(topology, topologyName, conf);
        }
    }

    protected abstract Config buildConfig();

    protected abstract StormTopology buildTopology();
    
    
    /*
    Run trident topology remotely
     */
    protected void runRemotely(StormTopology topology, String topologyName, Config conf) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        LOG.debug("run remotely");
        StormRunner.runTopologyRemotely(topology, topologyName, conf);
    }

    /*
    Run trident topology locally
     */
    protected void runLocally(StormTopology topology, String topologyName, Config conf) throws InterruptedException {
        LOG.debug("run remotely");
        LocalCluster cluster = new LocalCluster(LOCALCLUSTERHOST, LOCALCLUSTERPORT);
        StormRunner.runTopologyLocally(topology, topologyName, conf, RUNTIME_IN_SECONDS);
    }
}
