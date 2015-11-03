package com.devcycle.explorestorm.function;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.devcycle.explorestorm.message.MessageTuple;
import com.devcycle.explorestorm.util.JSONParser;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by chris howe-jones on 03/11/15.
 */
public class ParseAlertCBSMessage extends BaseFunction {

    private static final Logger LOG = LoggerFactory.getLogger(ParseAlertCBSMessage.class);
    private static final Fields EMITTED_FIELDS = new Fields(
            CBSMessageFields.FIELD_SEQNUM,
            CBSMessageFields.FIELD_T_IPPSTEM,
            CBSMessageFields.FIELD_T_IPTTST,
            CBSMessageFields.FIELD_T_IPTCLASS,
            CBSMessageFields.FIELD_T_HIACBL,
            CBSMessageFields.FIELD_T_IPTAM
    );
    private String fieldJsonString;

    private final JSONParser jsonParser = new JSONParser();

    /**
     * Create a ParseAlertCBSMessage function with the field name of the CBS message in the Trident Tuple
     *
     * @param fieldJsonString - tuple field name of input JSON message.
     */
    public ParseAlertCBSMessage(String fieldJsonString) {
        this.fieldJsonString = fieldJsonString;
    }

    public static Fields getEmittedFields() {
        return EMITTED_FIELDS;
    }

    /**
     * Execute the function to parse the input JSON in the tuple field (with the field name given in the
     * constructor) and emit the parsed fields of interest to the stream.
     *
     * @param tuple - input tuple consisting of a single field and String value of the JSON.
     * @param collector - collector used to interact with the stream.
     */
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String json = tuple.getStringByField(fieldJsonString);
        Values expectedValuesFromMessage = parseToTuples(json);
        collector.emit(expectedValuesFromMessage);
    }

    Values parseToTuples(String jsonMessage) {
        return new MessageTuple(parse(jsonMessage)).getValues();
    }

    /**
     * Parse a String representation of the JSON to a Map of field names and values for the fields of interest
     * in the JSON String.
     *
     * @param jsonMessage - JSON message as a String.
     * @return map of parsed fields and values
     */
    public Map<String, Object> parse(String jsonMessage) {
        Map<String, Object> fieldsMap = new LinkedHashMap<>();

        JSONObject jsonObject = null;
        try {
            jsonObject = new JSONObject(jsonMessage);
            fieldsMap.put(CBSMessageFields.FIELD_SEQNUM, jsonParser.parseInt(jsonObject, CBSMessageFields.FIELD_SEQNUM));
            fieldsMap.put(CBSMessageFields.FIELD_T_IPPSTEM, jsonParser.parseLong(jsonObject, CBSMessageFields.FIELD_T_IPPSTEM));
            fieldsMap.put(CBSMessageFields.FIELD_T_IPTTST, jsonParser.parseInt(jsonObject, CBSMessageFields.FIELD_T_IPTTST));
            fieldsMap.put(CBSMessageFields.FIELD_T_IPTCLASS, jsonParser.parseInt(jsonObject, CBSMessageFields.FIELD_T_IPTCLASS));
            fieldsMap.put(CBSMessageFields.FIELD_T_HIACBL, jsonParser.parseBigDecimal(jsonObject, CBSMessageFields.FIELD_T_HIACBL));
            fieldsMap.put(CBSMessageFields.FIELD_T_IPTAM, jsonParser.parseBigDecimal(jsonObject, CBSMessageFields.FIELD_T_IPTAM));
        } catch (JSONException e) {
            LOG.warn("Error in JSON: " + jsonMessage, e);
            fieldsMap = populateNullValues();
        }
        return fieldsMap;
    }

    private Map<String, Object> populateNullValues() {
        HashMap<String, Object> fieldsMap = new LinkedHashMap<>();
        fieldsMap.put(CBSMessageFields.FIELD_SEQNUM, null);
        fieldsMap.put(CBSMessageFields.FIELD_T_IPPSTEM, null);
        fieldsMap.put(CBSMessageFields.FIELD_T_IPTTST, null);
        fieldsMap.put(CBSMessageFields.FIELD_T_IPTCLASS, null);
        fieldsMap.put(CBSMessageFields.FIELD_T_HIACBL, null);
        return fieldsMap;
    }
}
