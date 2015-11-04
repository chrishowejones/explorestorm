package com.devcycle.explorestorm.scheme;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * Scheme for the lookup kafka topic used to interegate HBase
 *
 * Created by chrishowe-jones on 15/10/15.
 */
public class LookupScheme implements Scheme {

    private static final Logger LOG = LoggerFactory.getLogger(LookupScheme.class);
    public static final String FIELD_RETURN_COLUMNS = "returnColumns";
    public static final String FIELD_ROW_KEY = "ipAddress";
    public static final Fields fields = new Fields(
            FIELD_ROW_KEY

    );


    @Override
    public List<Object> deserialize(byte[] bytes) {
        try {
            String lookupEvent = new String(bytes, "UTF-8");
            String [] pieces = lookupEvent.split("\\|");

            String rowKey = cleanup(pieces[0]);
            return new Values(rowKey);
        } catch (UnsupportedEncodingException ex) {
            LOG.error("UTF-8 not supported", ex);
        }
         return null;
    }

    @Override
    public Fields getOutputFields() {
        return fields;
    }

    private String cleanup(String str) {
        if (str != null) {
            return str.trim().replace("\n", "").replace("\t", "");
        } else {
            return str;
        }
    }

}
