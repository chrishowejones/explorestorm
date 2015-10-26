package com.devcycle.explorestorm.message;

import backtype.storm.tuple.Values;
import org.apache.commons.digester.Rules;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Created by chris howe-jones on 21/10/15.
 */
public class MessageTupleTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void testCreateMessageTupleFromPopulatedMap() {
        Map<String, Object> fieldValuesMap = new HashMap<>();
        fieldValuesMap.put("test", "test message");
        MessageTuple tuple = new MessageTuple(fieldValuesMap);
        assertThat(tuple, notNullValue());
        assertThat(tuple.isEmpty(), is(false));
    }

    @Test
    public void testGetFields() {
        Map<String, Object> fieldValuesMap = new HashMap<>();
        final String test = "test";
        final String value = "test message";
        fieldValuesMap.put(test, value);
        final String again = "again";
        final String value1 = "another message";
        fieldValuesMap.put(again, value1);
        MessageTuple tuple = new MessageTuple(fieldValuesMap);
        assertThat(tuple.getFields(), notNullValue());
        assertThat(tuple.getFields().size(), is(2));
        assertThat(tuple.getFields().contains(test), is(true));
        assertThat(tuple.getFields().contains(again), is(true));
        assertThat(tuple.getFields().get(0), is(test));
        assertThat(tuple.getFields().get(1), is(again));
    }

    @Test
    public void testGetValues() {
        Map<String, Object> fieldValuesMap = new HashMap<>();
        final String test = "test";
        final String testValue = "test message";
        fieldValuesMap.put(test, testValue);
        final String anotherMessage = "another message";
        final String again = "again";
        fieldValuesMap.put(again, anotherMessage);

        MessageTuple tuple = new MessageTuple(fieldValuesMap);
        assertThat(tuple.getValues(), notNullValue());
        assertThat(tuple.getValues().size(), is(2));
        assertThat(tuple.getValues().contains(testValue), is(true));
        assertThat(tuple.getValues().contains(anotherMessage), is(true));
        assertThat((String)tuple.getValues().get(0), is(testValue));
        assertThat((String)tuple.getValues().get(1), is(anotherMessage));
    }

    @Test
    public void ReturnsEmptyValuesIfNullMapConstructor() {
        MessageTuple tuple = new MessageTuple(null);
        assertThat(tuple.getFields().size(), is(0));
        assertThat(tuple.getValues().size(), is(0));
     }

    @Test
    public void ReturnsEmptyValuesIfEmptyMapConstructor() {
        MessageTuple tuple = new MessageTuple(new HashMap<String, Object>());
        assertThat(tuple.getFields().size(), is(0));
        assertThat(tuple.getValues().size(), is(0));
    }

}