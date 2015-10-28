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

    private boolean empty = false;
    private Logger LOG = LoggerFactory.getLogger(MessageTuple.class);
    private Fields fields;
    private Values values;

    /**
     * Creates a MessageTuple from a Map of keys that represent Fields and Objects that represent Values.
     *
     * @param fieldValueMap - map of field names to object values that will be encapsulated by this tuple.
     */
    public MessageTuple(Map<String, Object> fieldValueMap) {
        LOG.trace("Created MessageTuple from map");
        setIsMapEmpty(fieldValueMap);
        loadKeyValues(fieldValueMap);
    }

    private void setIsMapEmpty(Map<String, Object> fieldValueMap) {
        if (fieldValueMap != null)
            empty = fieldValueMap.isEmpty();
    }

    private void loadKeyValues(Map<String, Object> fieldValueMap) {
        fields = buildFields(fieldValueMap);
        if (!empty && fieldValueMap.values().size() > 0) {
            values = new Values();
            values.addAll(fieldValueMap.values());
        }
    }

    private Fields buildFields(Map<String, Object> fieldValueMap) {
        if (fieldValueMap == null)
            return null;
        return new Fields(new ArrayList(fieldValueMap.keySet()));
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
