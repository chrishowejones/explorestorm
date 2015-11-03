package com.devcycle.explorestorm.mapper;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.verify;

/**
 * Created by chris howe-jones
 */
@RunWith(MockitoJUnitRunner.class)
public class OCISRowToValueMapperTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();
    @Mock
    private OutputFieldsDeclarer mockDeclarer;

    @Test
    public void testRowToValueMapperOneCFOneCol() {
        Map<String, Fields> cfFields = new HashMap<String, Fields>();
        String columnFamily = "cf";
        String columnName = "column1";
        Fields columns = new Fields(columnName);
        cfFields.put(columnFamily, columns);
        OCISRowToValueMapper mapper = new OCISRowToValueMapper(cfFields);
        assertThat(mapper, notNullValue());
        mapper.declareOutputFields(mockDeclarer);

        ArgumentCaptor<Fields> capturedFields = ArgumentCaptor.forClass(Fields.class);
        verify(mockDeclarer).declare(capturedFields.capture());
        assertThat(capturedFields.getValue().size(), is(1));
        assertThat(capturedFields.getValue().get(0), is(columnName));
    }

    @Test
    public void testRowToValueMapperOneCFTwoCols() {
        Map<String, Fields> cfFields = new HashMap<String, Fields>();
        String columnFamily = "cf";
        String column1 = "column1";
        String column2 = "column2";
        Fields columns = new Fields(column1, column2);
        cfFields.put(columnFamily, columns);
        OCISRowToValueMapper mapper = new OCISRowToValueMapper(cfFields);
        assertThat(mapper, notNullValue());
        mapper.declareOutputFields(mockDeclarer);

        ArgumentCaptor<Fields> capturedFields = ArgumentCaptor.forClass(Fields.class);
        verify(mockDeclarer).declare(capturedFields.capture());
        assertThat(capturedFields.getValue().size(), is(2));
        assertThat(capturedFields.getValue().get(0), is(column1));
        assertThat(capturedFields.getValue().get(1), is(column2));
    }

    @Test
    public void testRowToValueMapperTwoCFs() {
        Map<String, Fields> cfFields = new LinkedHashMap<String, Fields>();
        String columnFamily = "cf";
        String columnFamily2 = "cf2";
        String column1 = "column1";
        String column2 = "column2";
        String column3 = "column3";
        Fields columnsCF1 = new Fields(column1, column2);
        cfFields.put(columnFamily, columnsCF1);
        Fields columnsCF2 = new Fields(column3);
        cfFields.put(columnFamily2, columnsCF2);

        OCISRowToValueMapper mapper = new OCISRowToValueMapper(cfFields);
        assertThat(mapper, notNullValue());
        mapper.declareOutputFields(mockDeclarer);

        ArgumentCaptor<Fields> capturedFields = ArgumentCaptor.forClass(Fields.class);
        verify(mockDeclarer).declare(capturedFields.capture());
        assertThat(capturedFields.getValue().size(), is(3));
        assertThat(capturedFields.getValue().get(0), is(column1));
        assertThat(capturedFields.getValue().get(1), is(column2));
        assertThat(capturedFields.getValue().get(2), is(column3));
    }

    @Test
    public void testRowToValueMapperNullMap() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(containsString("cannot be null"));
        new OCISRowToValueMapper(null);
    }



}