package com.devcycle.explorestorm.message;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;

/**
 * Implements the concept of a lightweight tuple for messages.
 *
 * Created by chris howe-jones on 21/10/15.
 */
public class MessageTuple {

    private final boolean empty;
    private Logger LOG = LoggerFactory.getLogger(MessageTuple.class);
    private Fields fields;
    private Values values;

    /**
     * Creates a MessageTuple from a Map of keys that represent Fields and Objects that represent Values.
     *
     * @param fieldValueMap - map of field names to object values that will be encapsulated by this tuple.
     */
    public MessageTuple(Map<String, Object> fieldValueMap) {
        if (fieldValueMap == null)
            throw new IllegalArgumentException("Field Value Map cannot be null.");
        if (fieldValueMap.isEmpty())
            throw new IllegalArgumentException("Field Value Map cannot be empty");
        LOG.trace("Created MessageTuple from map");
        empty = fieldValueMap.isEmpty();
        loadKeyValues(fieldValueMap);
    }

    private void loadKeyValues(Map<String, Object> fieldValueMap) {
        fields = new Fields(new ArrayList(fieldValueMap.keySet()));
        values = new Values();
        values.addAll(fieldValueMap.values());
    }

    /**
     * Checks if this message tuple is empty (has no Fields or Values).
     *
     * @return true if empty, false otherwise.
     */
    public boolean isEmpty() {
        return empty;
    }

    /**
     * Returns Fields for this tuple.
     *
     * @return Fields in this tuple.
     */
    public Fields getFields() {
        return fields;
    }

    public Values getValues() {
        return values;
    }
}
