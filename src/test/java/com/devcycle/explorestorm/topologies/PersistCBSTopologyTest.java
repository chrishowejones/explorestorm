package com.devcycle.explorestorm.topologies;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import com.devcycle.explorestorm.function.ParseCBSMessage;
import com.devcycle.explorestorm.scheme.CBSKafkaScheme;
import com.devcycle.explorestorm.util.HBaseConfigBuilder;
import org.apache.storm.hbase.trident.state.HBaseStateFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import storm.kafka.trident.OpaqueTridentKafkaSpout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

import static com.devcycle.explorestorm.topologies.HBaseConfig.HBASE_CONFIG;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;

/**
 * Created by chris howe-jones on 21/10/15.
 */
@RunWith(MockitoJUnitRunner.class)
public class PersistCBSTopologyTest {

    @Mock
    private HBaseConfigBuilder mockHBaseConfigBuilder;
    @Mock
    private Properties mockConfigProperties;

    @Test
    public void testBuildConfig() throws IOException {
        PersistCBSTopology topology = new PersistCBSTopology("");
        Config config = topology.buildConfig();
        assertThat(config, notNullValue());
        assertThat(config, hasEntry(is(HBASE_CONFIG.toString()), notNullValue()));
    }

    @Test
    public void testBuildConfigHBaseConnection() throws IOException {
        // mock znode parent
        final String znodeParent = "test.znode";
        givenHBaseConfigBuilder();
        PersistCBSTopology topology = new PersistCBSTopology("");
        // inject mock HBaseConfigBuilder
        topology.setHBaseConfigBuilder(mockHBaseConfigBuilder);

        Config config = topology.buildConfig();
        @SuppressWarnings("unchecked") HashMap<String, Object> hbaseConfig = (HashMap<String, Object>) config.get(HBASE_CONFIG.toString());
        assertThat(hbaseConfig.get(HBaseConfigBuilder.ZOOKEEPER_ZNODE_PARENT), is((Object) znodeParent));
    }

    @Test
    public void testBuildTopology() throws IOException {
        PersistCBSTopology topology = new PersistCBSTopology(new Properties());
        topology.setCbsKafkaScheme(new CBSKafkaScheme());
        // assert that topology returned with KafkaSpout configured.
        final StormTopology stormTopology = topology.buildTopology();
        assertThat(stormTopology, notNullValue());
        assertThat(stormTopology.get_spouts(), notNullValue());
        assertThat(stormTopology.get_spouts().size(), is(1));
        assertThat(stormTopology.get_bolts().size(), is(3));
        assertThat(stormTopology.get_state_spouts_size(), is(0));
    }

    @Test
    public void testBuildHBaseStateFactory() {
        PersistCBSTopology topology = new PersistCBSTopology(mockConfigProperties);
        HBaseStateFactory factory = topology.buildCBSHBaseStateFactory(new ArrayList<String>());
        assertThat(factory, notNullValue());
    }

    private void givenHBaseConfigBuilder() {
        HashMap<String, Object> expectedHBaseConfig = new HashMap<String, Object>();
        expectedHBaseConfig.put(HBaseConfigBuilder.ZOOKEEPER_ZNODE_PARENT, "test.znode");

        when(mockHBaseConfigBuilder.getHBaseConfig(any(Properties.class)))
        .thenReturn(expectedHBaseConfig);
    }

}