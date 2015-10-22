package com.devcycle.explorestorm.topologies;

/**
 * Encapsulates the HBase Configuration keys.
 *
 * Created by chrishowe-jones on 22/10/15.
 */
public enum KafkaConfig {

    KAFKA_ZOOKEEPER_HOST_PORT ("kafka.zookeeper.host.port"),
    HBASE_CONFIG ("hbase.config");

    private final String key;

    KafkaConfig(String propertyKey) {
        this.key = propertyKey;
    }

    @Override
    public String toString() {
        return key;
    }
}
