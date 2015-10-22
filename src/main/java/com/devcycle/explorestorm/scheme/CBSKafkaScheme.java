package com.devcycle.explorestorm.scheme;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * Created by chrishowe-jones on 22/10/15.
 */
public class CBSKafkaScheme implements backtype.storm.spout.Scheme {

    public static final String FIELD_JSON_MESSAGE = "cbsMessage";
    private static final Logger LOG = LoggerFactory.getLogger(CBSKafkaScheme.class);

    @Override
    public List<Object> deserialize(byte[] bytes) {
        String jsonMessage = null;
        Values values = null;
        try {
            jsonMessage = new String(bytes, "UTF-8");
            values = new Values(jsonMessage);
        } catch (UnsupportedEncodingException e) {
            LOG.error("Not UTF-8", e);
        }
        return values;
    }

    @Override
    public Fields getOutputFields() {
        LOG.trace("getOutputFields");
        return new Fields(
                FIELD_JSON_MESSAGE
        );
    }
}
