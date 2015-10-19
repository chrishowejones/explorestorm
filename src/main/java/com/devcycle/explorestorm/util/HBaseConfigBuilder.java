package com.devcycle.explorestorm.util;

import java.util.HashMap;
import java.util.Properties;

/**
 * Create HBase Configuration.
 * <p/>
 * Created by chrishowe-jones on 14/10/15.
 */
public class HBaseConfigBuilder {

    public static final String HBASE_ROOTDIR = "hbase.rootdir";
    public static final String HBASE_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
    public static final String ZOOKEEPER_ZNODE_PARENT = "zookeeper.znode.parent";


    public HashMap<String, Object> getHBaseConfig(Properties topologyConfig) {
        HashMap<String, Object> hbaseConfig = new HashMap<String, Object>();
        hbaseConfig.put(HBASE_ROOTDIR, topologyConfig.getProperty(HBASE_ROOTDIR));
        hbaseConfig.put(HBASE_ZOOKEEPER_QUORUM, "hostgroupmaster1-3-lloyds-20150923072909.node.dc1.consul," +
                "hostgroupmaster3-4-lloyds-20150923072909.node.dc1.consul," +
                "hostgroupmaster2-2-lloyds-20150923072909.node.dc1.consul");
        hbaseConfig.put(ZOOKEEPER_ZNODE_PARENT, topologyConfig.getProperty(ZOOKEEPER_ZNODE_PARENT));
        return hbaseConfig;
    }


}
