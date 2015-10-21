package com.devcycle.explorestorm.message;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Created by chris howe-jones on 21/10/15.
 */
public class MessageTupleTest {

    @Test
    public void testCreateMessageTupleFromEmptyMap() {
        Map<String,Object> fieldValuesMap = new HashMap<String, Object>();
        MessageTuple tuple = new MessageTuple(fieldValuesMap);
        assertThat(tuple, notNullValue());
        assertThat(tuple.isEmpty(), is(true));
    }

    @Test
    public void testCreateMessageTupleFromPopulatedMap() {
        Map<String, Object> fieldValuesMap = new HashMap<>();
        fieldValuesMap.put("test", "test message");
        MessageTuple tuple = new MessageTuple(fieldValuesMap);
        assertThat(tuple, notNullValue());
        assertThat(tuple.isEmpty(), is(false));
    }



}