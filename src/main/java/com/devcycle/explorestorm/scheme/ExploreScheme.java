package com.devcycle.explorestorm.scheme;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.tuple.TridentTuple;


import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * Created by chrishowe-jones on 17/09/15.
 */
public class ExploreScheme implements Scheme {

    private static final Logger LOG = LoggerFactory.getLogger(ExploreScheme.class);
    public static final String FIELD_MESSAGE = "message";
    public static final String FIELD_EVENT_TIME = "eventTime";
    public static final String FIELD_IP_ADDRESS = "ipAddress";
    private Fields fields = new Fields(
            FIELD_EVENT_TIME,
            FIELD_MESSAGE,
            FIELD_IP_ADDRESS);

    public List<Object> deserialize(byte[] bytes) {
        try {
            String exploreEvent = new String(bytes, "UTF-8");
            String[] pieces = exploreEvent.split("\\,");

            Date eventDate = new Date(Long.parseLong(pieces[0]));
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
            String eventTime = format.format(eventDate);
            String message = pieces[1];
            String ip = pieces[2];
            return new Values(eventTime, cleanup(message), cleanup(ip));

        } catch (UnsupportedEncodingException e) {
            LOG.error("UnsupportedEncodingException", e);
        } catch (IllegalArgumentException ex) {
            LOG.error("Date invalid ", ex);
        }
        return null;
    }

    private String cleanup(String str) {
        if (str != null) {
            return str.trim().replace("\n", "").replace("\t", "");
        } else {
            return str;
        }

    }

    public Fields getOutputFields() {
        return fields;
    }

    public static String prettyPrintTuple(Tuple tuple) {
        return tuple.getStringByField(ExploreScheme.FIELD_EVENT_TIME) + "," +
                tuple.getStringByField(ExploreScheme.FIELD_MESSAGE) + "," +
                tuple.getStringByField(ExploreScheme.FIELD_IP_ADDRESS);
    }

    public static String prettyPrintTuple(TridentTuple tuple) {
        return "event time = " + tuple.getStringByField(ExploreScheme.FIELD_EVENT_TIME) + "," +
                "message = " + tuple.getStringByField(ExploreScheme.FIELD_MESSAGE) + "," +
                "IP = " + tuple.getStringByField(ExploreScheme.FIELD_IP_ADDRESS);
    }


}
