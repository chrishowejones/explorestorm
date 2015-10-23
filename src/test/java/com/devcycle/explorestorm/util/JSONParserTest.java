package com.devcycle.explorestorm.util;

import org.apache.storm.jetty.util.ajax.JSON;
import org.json.JSONObject;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.*;

/**
 * Created by chrishowe-jones on 23/10/15.
 */
public class JSONParserTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void testParseDateString() throws Exception {
        JSONParser parser = new JSONParser();
        String dateJSON = "{\"datefield\":151001}";
        JSONObject jsonObject = new JSONObject(dateJSON);
        assertThat(parser.parseDateString(jsonObject, "datefield"), is("2015-10-01"));
    }

    @Test
    public void testParseInvalidDateString() throws Exception {
        JSONParser parser = new JSONParser();
        String dateJSON = "{\"datefield\":\"\"}";
        JSONObject jsonObject = new JSONObject(dateJSON);
        assertThat(parser.parseDateString(jsonObject, "datefield"), nullValue());
    }
}