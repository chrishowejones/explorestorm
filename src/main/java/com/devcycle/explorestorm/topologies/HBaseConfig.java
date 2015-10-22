package com.devcycle.explorestorm.topologies;

/**
 * Encapsulates the HBase Configuration keys.
 *
 * Created by chrishowe-jones on 22/10/15.
 */
public enum HBaseConfig {

    HBASE_ROOT ("hbase.rootdir"),
    HBASE_CONFIG ("hbase.config");

    private final String key;

    HBaseConfig(String propertyKey) {
        this.key = propertyKey;
    }

    @Override
    public String toString() {
        return key;
    }
}
