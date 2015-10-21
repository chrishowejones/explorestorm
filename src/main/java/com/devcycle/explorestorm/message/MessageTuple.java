package com.devcycle.explorestorm.message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Implements the concept of a lightweight tuple for messages.
 *
 * Created by chris howe-jones on 21/10/15.
 */
public class MessageTuple {

    private final boolean empty;
    private Logger LOG = LoggerFactory.getLogger(MessageTuple.class);

    /**
     * Creates a MessageTuple from a Map of keys that represent Fields and Objects that represent Values.
     *
     * @param fieldValueMap - map of field names to object values that will be encapsulated by this tuple.
     */
    public MessageTuple(Map<String, Object> fieldValueMap) {
        LOG.trace("Created MessageTuple from map");
        empty = fieldValueMap.isEmpty();
    }

    public boolean isEmpty() {

        return empty;
    }
}
