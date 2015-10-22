package com.devcycle.explorestorm.scheme;

import backtype.storm.tuple.Values;
import org.junit.Test;

import java.util.List;

import static com.devcycle.explorestorm.scheme.CBSKafkaScheme.FIELD_JSON_MESSAGE;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

/**
 * Created by chrishowe-jones on 22/10/15.
 */
public class CBSKafkaSchemeTest {

    @Test
    public void testGetOutputFields() {
        CBSKafkaScheme scheme = new CBSKafkaScheme();
        assertThat(scheme.getOutputFields(), notNullValue());
        assertThat(scheme.getOutputFields().size(), is(1));
        assertThat(scheme.getOutputFields().get(0), is(FIELD_JSON_MESSAGE));
    }

    @Test
    public void testDeserialize() {
        String jsonMessage = "{\"testField\":\"testing...\"}";
        byte[] jsonMessageBytes = jsonMessage.getBytes();

        CBSKafkaScheme scheme = new CBSKafkaScheme();
        final List<Object> deserialize = scheme.deserialize(jsonMessageBytes);
        assertThat(deserialize, notNullValue());
        assertThat(deserialize, instanceOf(Values.class));
        assertThat(deserialize, hasSize(1));
        assertThat((String)deserialize.get(0), is(jsonMessage));
    }

}