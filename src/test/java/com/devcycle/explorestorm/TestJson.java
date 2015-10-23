package com.devcycle.explorestorm;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by chrishowe-jones on 20/10/15.
 */
public class TestJson {


    private Scanner scan;


    @Before
    public void setUpClass() throws FileNotFoundException {
        ClassLoader classLoader = getClass().getClassLoader();
        String filepath = classLoader.getResource("testmsg.txt").getFile();
        File file = new File(filepath);
        scan = new Scanner(file);
    }

    private JSONObject parseLine(String line) throws JSONException {
        return new JSONObject(line);
    }

    private String readLine() {
        String line = null;
        if(scan.hasNext())
            line = scan.next();
        return line;
    }

    @Test
    public void testSimpleParse() throws JSONException {
        String simpleJson = "{\"name\":\"Chris\", \"age\":50}\n";
        JSONObject line = parseLine(simpleJson);
        assertThat(line.length(), is(2));
        assertThat(line.has("name"), is(true));
        assertThat(line.getString("name"), is("Chris"));
        assertThat(line.has("age"), is(true));
        assertEquals(50, line.getInt("age"));
    }

    @Test
    public void testFirstLine() throws JSONException {
        JSONObject line = parseLine(readLine());
        assertThat(line.length(), is(1240));
        assertThat(line.getInt("SEQNUM"), is(1));
        assertThat(line.getLong("tIPTETIME"), is(153236L));
        assertThat(line.getInt("tIPPBR"), is(0));
        assertThat(line.getLong("tIPPSTEM"), is(0L));
        assertThat(line.getLong("tIPTTST"), is(0L));
        assertThat(line.getLong("tIPTCLCDE"), is(0L));
        assertThat(line.getLong("tIPTAM"), is(0L));
        assertThat(line.getLong("tIPCURCDE"), is(0L));
        assertThat(line.getLong("tHIACBL"), is(165119L));
        assertThat(line.getLong("tIPCDATE"), is(151013L));
        assertThat(line.get("tIPTD"), is(JSONObject.NULL));
        assertThat(line.has("tIPTXNARR"), is(false));
    }

    @Test
    public void testSecondLine() throws JSONException {
        readLine();
        JSONObject line = parseLine(readLine());
        assertThat(line.length(), is(1240));
        assertThat(line.getInt("SEQNUM"), is(2));
        assertThat(line.getInt("tIPPBR"), is(0));
        assertThat(line.getLong("tIPPSTEM"), is(0L));
        assertThat(line.getLong("tIPTTST"), is(0L));
        assertThat(line.getLong("tIPTCLCDE"), is(0L));
        assertThat(line.getLong("tIPTAM"), is(0L));
        assertThat(line.getLong("tIPCURCDE"), is(0L));
        assertThat(line.getLong("tHIACBL"), is(165119L));
        assertThat(line.getLong("tIPCDATE"), is(151013L));
        assertThat(line.get("tIPTD"), is(JSONObject.NULL));
        assertThat(line.has("tIPTXNARR"), is(false));
    }

    @Test
    public void testThirdLine() throws JSONException {
        readLine();
        readLine();
        JSONObject line = parseLine(readLine());
        assertThat(line.length(), is(1241));
        assertThat(line.getInt("SEQNUM"), is(3));
        assertThat(line.getInt("tIPPBR"), is(0));
        assertThat(line.getLong("tIPPSTEM"), is(0L));
        assertThat(line.getLong("tIPTTST"), is(201L));
        assertThat(line.getLong("tIPTCLCDE"), is(65737L));
        assertThat(line.getLong("tIPTAM"), is(0L));
        assertThat(line.getLong("tIPCURCDE"), is(0L));
        assertThat(line.getLong("tHIACBL"), is(165119L));
        assertThat(line.getLong("tIPCDATE"), is(151013L));
        assertThat(line.get("tIPTD"), is(JSONObject.NULL));
        assertThat(line.has("tIPTXNARR"), is(false));
    }

    @Test
    public void testSpacesInJSON() throws JSONException {
        String spaces = "{\"spaces\":\"    \"}";
        JSONObject jsonObject = new JSONObject(spaces);
        assertThat(jsonObject.getString("spaces"), is("    "));

    }
}
