package com.devcycle.explorestorm.topologies;

import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by chrishowe-jones on 17/09/15.
 */
public class BaseExploreTopology {

    private static final Logger LOG = Logger.getLogger(BaseExploreTopology.class);

    protected Properties topologyConfig;

    public BaseExploreTopology(String configFileLocation) throws IOException {
        topologyConfig = new Properties();
        topologyConfig.load(ClassLoader.getSystemResourceAsStream(configFileLocation));
    }
}
