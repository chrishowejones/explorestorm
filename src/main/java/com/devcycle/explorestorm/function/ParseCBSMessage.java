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
 * Trident function to parse CBS message from a String.
 * <p/>
 * Created by chrishowe-jones on 20/10/15.
 */
public class ParseCBSMessage extends BaseFunction {

    private static final Logger LOG = LoggerFactory.getLogger(ParseCBSMessage.class);
    private static final Fields EMITTED_FIELDS = new Fields(
            CBSMessageFields.FIELD_SEQNUM,
            CBSMessageFields.FIELD_T_IPTETIME,
            CBSMessageFields.FIELD_T_IPPBR,
            CBSMessageFields.FIELD_T_IPPSTEM,
            CBSMessageFields.FIELD_T_IPTTST,
            CBSMessageFields.FIELD_T_IPTCLCDE,
            CBSMessageFields.FIELD_T_IPTAM,
            CBSMessageFields.FIELD_T_IPCURCDE,
            CBSMessageFields.FIELD_T_HIACBL,
            CBSMessageFields.FIELD_T_IPCDATE,
            CBSMessageFields.FIELD_T_IPTD,
            CBSMessageFields.FIELD_T_IPTXNARR,
            CBSMessageFields.FIELD_FULL_MESSAGE
    );
    private final JSONParser jsonParser = new JSONParser();
    private String fieldJsonString;

    /**
     * Create a ParseCBSMessage function with the field name of the CBS message in the Trident Tuple
     *
     * @param fieldJsonString - tuple field name of input JSON message.
     */
    public ParseCBSMessage(String fieldJsonString) {
        this.fieldJsonString = fieldJsonString;
    }

    /**
     * Get the emitted fields from this function.
     *
     * @return fields emitted from this function.
     */
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
            fieldsMap.put(CBSMessageFields.FIELD_T_IPTETIME, jsonParser.parseLong(jsonObject, CBSMessageFields.FIELD_T_IPTETIME));
            fieldsMap.put(CBSMessageFields.FIELD_T_IPPBR, jsonParser.parseInt(jsonObject, CBSMessageFields.FIELD_T_IPPBR));
            fieldsMap.put(CBSMessageFields.FIELD_T_IPPSTEM, jsonParser.parseLong(jsonObject, CBSMessageFields.FIELD_T_IPPSTEM));
            fieldsMap.put(CBSMessageFields.FIELD_T_IPTTST, jsonParser.parseInt(jsonObject, CBSMessageFields.FIELD_T_IPTTST));
            fieldsMap.put(CBSMessageFields.FIELD_T_IPTCLCDE, jsonParser.parseInt(jsonObject, CBSMessageFields.FIELD_T_IPTCLCDE));
            fieldsMap.put(CBSMessageFields.FIELD_T_IPTAM, jsonParser.parseBigDecimal(jsonObject, CBSMessageFields.FIELD_T_IPTAM));
            fieldsMap.put(CBSMessageFields.FIELD_T_IPCURCDE, jsonParser.parseInt(jsonObject, CBSMessageFields.FIELD_T_IPCURCDE));
            fieldsMap.put(CBSMessageFields.FIELD_T_HIACBL, jsonParser.parseBigDecimal(jsonObject, CBSMessageFields.FIELD_T_HIACBL));
            fieldsMap.put(CBSMessageFields.FIELD_T_IPCDATE, jsonParser.parseDateString(jsonObject, CBSMessageFields.FIELD_T_IPCDATE));
            fieldsMap.put(CBSMessageFields.FIELD_T_IPTD, jsonParser.parseDateString(jsonObject, CBSMessageFields.FIELD_T_IPTD));
            fieldsMap.put(CBSMessageFields.FIELD_T_IPTXNARR, jsonParser.parseString(jsonObject, CBSMessageFields.FIELD_T_IPTXNARR));
            fieldsMap.put(CBSMessageFields.FIELD_FULL_MESSAGE, jsonMessage);
        } catch (JSONException e) {
            LOG.warn("Error in JSON: " + jsonMessage, e);
            fieldsMap = populateNullValues();
        }
        return fieldsMap;
    }

    private Map<String, Object> populateNullValues() {
        HashMap<String, Object> fieldsMap = new LinkedHashMap<>();
        fieldsMap.put(CBSMessageFields.FIELD_SEQNUM, null);
        fieldsMap.put(CBSMessageFields.FIELD_T_IPTETIME, null);
        fieldsMap.put(CBSMessageFields.FIELD_T_IPPBR, null);
        fieldsMap.put(CBSMessageFields.FIELD_T_IPPSTEM, null);
        fieldsMap.put(CBSMessageFields.FIELD_T_IPTTST, null);
        fieldsMap.put(CBSMessageFields.FIELD_T_IPTCLCDE, null);
        fieldsMap.put(CBSMessageFields.FIELD_T_IPTAM, null);
        fieldsMap.put(CBSMessageFields.FIELD_T_IPCURCDE, null);
        fieldsMap.put(CBSMessageFields.FIELD_T_HIACBL, null);
        fieldsMap.put(CBSMessageFields.FIELD_T_IPCDATE, null);
        fieldsMap.put(CBSMessageFields.FIELD_T_IPTD, null);
        fieldsMap.put(CBSMessageFields.FIELD_T_IPTXNARR, null);
        fieldsMap.put(CBSMessageFields.FIELD_FULL_MESSAGE, null);
        return fieldsMap;
    }
}

