package com.devcycle.explorestorm.function;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.devcycle.explorestorm.message.MessageTuple;
import com.devcycle.explorestorm.scheme.CBSMessageFields;
import com.devcycle.explorestorm.util.JSONParser;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Trident function to parse CBS message from a String.
 * <p/>
 * Created by chrishowe-jones on 20/10/15.
 */
public class ParseCBSMessage extends BaseFunction {

    private static final Logger LOG = LoggerFactory.getLogger(ParseCBSMessage.class);

    private final JSONParser jsonParser = new JSONParser();
    private List<String> fields;
    private String fieldJsonString;
    private CBSMessageFields cbsMessageFields = new CBSMessageFields();

    /**
     * Create a ParseCBSMessage function with the field name of the CBS message in the Trident Tuple
     *
     * @param fieldJsonString - tuple field name of input JSON message.
     */
    @Deprecated
    public ParseCBSMessage(String fieldJsonString) {
        initialise(fieldJsonString);
    }

    /**
     * Create a ParseCBSMessage function with the field name of the CBS message and the list of fields to parse
     *
     * @param fieldJsonString field name to use to read the json string
     * @param fields          to parse from the json string
     */
    public ParseCBSMessage(String fieldJsonString, List<String> fields) {
        initialise(fieldJsonString);
        this.fields = fields;
    }

    /**
     * Execute the function to parse the input JSON in the tuple field (with the field name given in the
     * constructor) and emit the parsed fields of interest to the stream.
     *
     * @param tuple     - input tuple consisting of a single field and String value of the JSON.
     * @param collector - collector used to interact with the stream.
     */
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String json = tuple.getStringByField(fieldJsonString);
        Values expectedValuesFromMessage = parseToTuples(json);
        collector.emit(expectedValuesFromMessage);
    }

    /**
     * Parse a String representation of the JSON to a Map of field names and values for the fields of interest
     * in the JSON String.
     *
     * @param jsonMessage - JSON message as a String.
     * @return map of parsed fields and values
     */
    public Map<String, Object> parse(String jsonMessage) {
        Map<String, Object> fieldsMap = null;

        JSONObject jsonObject = null;
        try {
            jsonObject = new JSONObject(jsonMessage);
            fieldsMap = buildFieldsMap(jsonObject);
        } catch (JSONException e) {
            LOG.warn("Error in JSON: " + jsonMessage, e);
            fieldsMap = populateNullValues();
        }
        return fieldsMap;
    }

    private Map<String, Object> buildFieldsMap(JSONObject jsonObject) throws JSONException {
        Map<String, Object> fieldsMap = new LinkedHashMap<>();
        if (fields != null) {
            for (String field : fields) {
                fieldsMap.put(field, parseField(jsonObject, field, cbsMessageFields.getType(field)));
            }
        }
        return fieldsMap;
    }

    private Object parseField(JSONObject jsonObject, String key, Class type) throws JSONException {
        if (type.equals(Long.class))
            return jsonParser.parseLong(jsonObject, key);
        if (type.equals(String.class))
            return jsonParser.parseString(jsonObject, key);
        if (type.equals(Integer.class))
            return jsonParser.parseInt(jsonObject, key);
        if (type.equals(BigDecimal.class))
            return jsonParser.parseBigDecimal(jsonObject, key);
        if (type.equals(JSONObject.class))
            return jsonObject.toString();
        return null;
    }

    Values parseToTuples(String jsonMessage) {
        return new MessageTuple(parse(jsonMessage)).getValues();
    }

    List<String> getFields() {
        return fields;
    }

    private void initialise(String fieldJsonString) {
        this.fieldJsonString = fieldJsonString;
    }

    private Map<String, Object> populateNullValues() {
        return buildNullFieldsMap();
    }

    private Map<String, Object> buildNullFieldsMap() {
        Map<String, Object> fieldsMap = new LinkedHashMap<>();
        if (fields != null) {
            for (String field : fields) {
                fieldsMap.put(field, null);
            }
        }
        return fieldsMap;
    }
}

