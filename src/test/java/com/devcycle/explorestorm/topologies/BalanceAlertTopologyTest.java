package com.devcycle.explorestorm.topologies;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import com.devcycle.explorestorm.scheme.CBSKafkaScheme;
import com.devcycle.explorestorm.util.HBaseConfigBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import storm.trident.TridentTopology;

import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;

import static com.devcycle.explorestorm.topologies.HBaseConfig.HBASE_CONFIG;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.notNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by chris howe-jones on 29/10/15.
 */
@RunWith(MockitoJUnitRunner.class)
public class BalanceAlertTopologyTest {

    @Mock
    private HBaseConfigBuilder mockHBaseConfigBuilder;
    @Mock
    private Properties mockConfigProperties;

    @Before
    public void setUp() {
        when(mockConfigProperties.getProperty(BalanceAlertTopology.METADATA_BROKER_LIST)).thenReturn("hostgroupslave2-8-lloyds-20150923072910:6667");
        when(mockConfigProperties.getProperty(BalanceAlertTopology.REQUEST_REQUIRED_ACKS)).thenReturn("1");
    }


    @Test
    public void testBuildConfig() throws IOException {
        BalanceAlertTopology topology = new BalanceAlertTopology(mockConfigProperties);

        Config config = topology.buildConfig();
        assertThat(config, notNullValue());
        assertThat(config, hasEntry(is(HBASE_CONFIG.toString()), notNullValue()));
    }

    @Test
    public void testBuildConfigHBaseConnection() throws IOException {
        // mock znode parent
        final String znodeParent = "test.znode";
        givenHBaseConfigBuilder();
        BalanceAlertTopology topology = new BalanceAlertTopology(mockConfigProperties);
        // inject mock HBaseConfigBuilder
        topology.setHBaseConfigBuilder(mockHBaseConfigBuilder);

        Config config = topology.buildConfig();
        HashMap<String, Object> hbaseConfig = (HashMap<String, Object>) config.get(HBASE_CONFIG.toString());
        assertThat(hbaseConfig.get(HBaseConfigBuilder.ZOOKEEPER_ZNODE_PARENT), is((Object) znodeParent));
    }

    @Test
    public void testBuildTopology() throws IOException {
        BalanceAlertTopology topology = new BalanceAlertTopology(mockConfigProperties);
        topology.setCbsKafkaScheme(new CBSKafkaScheme());
        // assert that topology returned with KafkaSpout configured.
        final StormTopology stormTopology = topology.buildTopology();
        assertThat(stormTopology, notNullValue());
        assertThat(stormTopology.get_spouts(), notNullValue());
        assertThat(stormTopology.get_spouts().size(), is(1));
        assertThat(stormTopology.get_bolts().size(), is(3));
    }

    private void givenHBaseConfigBuilder() {
        HashMap<String, Object> expectedHBaseConfig = new HashMap<String, Object>();
        expectedHBaseConfig.put(HBaseConfigBuilder.ZOOKEEPER_ZNODE_PARENT, "test.znode");

        when(mockHBaseConfigBuilder.getHBaseConfig(any(Properties.class)))
                .thenReturn(expectedHBaseConfig);
    }
}