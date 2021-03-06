package com.devcycle.explorestorm.util;

import org.apache.storm.jetty.util.ajax.JSON;
import org.json.JSONObject;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.math.BigDecimal;
import java.math.BigInteger;

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

    @Test
    public void testParseDate() throws Exception {
        JSONParser parser = new JSONParser();
        String dateJSON = "{\"datefield\":\"2015-11-01\"}";
        JSONObject jsonObject = new JSONObject(dateJSON);
        assertThat(parser.parseString(jsonObject, "datefield"), is("2015-11-01"));
    }

    @Test
    public void testParseBigDecimal() throws Exception {
        JSONParser parser = new JSONParser();
        String bigdecimal = "{\"bigdecimal\":12345.67}";
        JSONObject jsonObject = new JSONObject(bigdecimal);
        assertThat(parser.parseBigDecimal(jsonObject, "bigdecimal"), is(new BigDecimal("12345.67").setScale(2)));
    }
}