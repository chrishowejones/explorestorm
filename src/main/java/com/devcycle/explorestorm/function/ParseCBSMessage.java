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
            CBSMessageFields.FIELD_TIME,
            CBSMessageFields.FIELD_BRANCH_CODE,
            CBSMessageFields.FIELD_PARENT_STEM,
            CBSMessageFields.FIELD_TXN_TYPE,
            CBSMessageFields.FIELD_TXN_CODE,
            CBSMessageFields.FIELD_TXN_AMOUNT,
            CBSMessageFields.FIELD_CURRENCY_CDE,
            CBSMessageFields.FIELD_CURRENT_ACCOUNT_BALANCE,
            CBSMessageFields.FIELD_CURRENT_DATE,
            CBSMessageFields.FIELD_TXN_DATE,
            CBSMessageFields.FIELD_TXN_NARRATIVE,
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
            fieldsMap.put(CBSMessageFields.FIELD_SEQNUM, jsonParser.parseLong(jsonObject, CBSMessageFields.FIELD_SEQNUM));
            fieldsMap.put(CBSMessageFields.FIELD_TIME, jsonParser.parseString(jsonObject, CBSMessageFields.FIELD_TIME));
            fieldsMap.put(CBSMessageFields.FIELD_ACCOUNT_NUMBER, jsonParser.parseLong(jsonObject, CBSMessageFields.FIELD_ACCOUNT_NUMBER));
            fieldsMap.put(CBSMessageFields.FIELD_TXN_TYPE, jsonParser.parseInt(jsonObject, CBSMessageFields.FIELD_TXN_TYPE));
            fieldsMap.put(CBSMessageFields.FIELD_TXN_CODE, jsonParser.parseInt(jsonObject, CBSMessageFields.FIELD_TXN_CODE));
            fieldsMap.put(CBSMessageFields.FIELD_TXN_AMOUNT, jsonParser.parseBigDecimal(jsonObject, CBSMessageFields.FIELD_TXN_AMOUNT));
            fieldsMap.put(CBSMessageFields.FIELD_CURRENCY_CDE, jsonParser.parseInt(jsonObject, CBSMessageFields.FIELD_CURRENCY_CDE));
            fieldsMap.put(CBSMessageFields.FIELD_CURRENT_ACCOUNT_BALANCE, jsonParser.parseBigDecimal(jsonObject, CBSMessageFields.FIELD_CURRENT_ACCOUNT_BALANCE));
            fieldsMap.put(CBSMessageFields.FIELD_CURRENT_DATE, jsonParser.parseString(jsonObject, CBSMessageFields.FIELD_CURRENT_DATE));
            fieldsMap.put(CBSMessageFields.FIELD_TXN_DATE, jsonParser.parseString(jsonObject, CBSMessageFields.FIELD_TXN_DATE));
            fieldsMap.put(CBSMessageFields.FIELD_TXN_NARRATIVE, jsonParser.parseString(jsonObject, CBSMessageFields.FIELD_TXN_NARRATIVE));
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
        fieldsMap.put(CBSMessageFields.FIELD_TIME, null);
        fieldsMap.put(CBSMessageFields.FIELD_BRANCH_CODE, null);
        fieldsMap.put(CBSMessageFields.FIELD_PARENT_STEM, null);
        fieldsMap.put(CBSMessageFields.FIELD_TXN_TYPE, null);
        fieldsMap.put(CBSMessageFields.FIELD_TXN_CODE, null);
        fieldsMap.put(CBSMessageFields.FIELD_TXN_AMOUNT, null);
        fieldsMap.put(CBSMessageFields.FIELD_CURRENCY_CDE, null);
        fieldsMap.put(CBSMessageFields.FIELD_CURRENT_ACCOUNT_BALANCE, null);
        fieldsMap.put(CBSMessageFields.FIELD_CURRENT_DATE, null);
        fieldsMap.put(CBSMessageFields.FIELD_TXN_DATE, null);
        fieldsMap.put(CBSMessageFields.FIELD_TXN_NARRATIVE, null);
        fieldsMap.put(CBSMessageFields.FIELD_FULL_MESSAGE, null);
        return fieldsMap;
    }
}

