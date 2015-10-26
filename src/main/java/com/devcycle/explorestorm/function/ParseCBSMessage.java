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

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Trident function to parse CBS message from a String.
 * <p/>
 * Created by chrishowe-jones on 20/10/15.
 */
public class ParseCBSMessage extends BaseFunction {

    public static final String FIELD_SEQNUM = "SEQNUM";
    public static final String FIELD_T_IPTETIME = "tIPTETIME";
    public static final String FIELD_T_IPPBR = "tIPPBR";
    public static final String FIELD_T_IPPSTEM = "tIPPSTEM";
    public static final String FIELD_T_IPTTST = "tIPTTST";
    public static final String FIELD_T_IPTCLCDE = "tIPTCLCDE";
    public static final String FIELD_T_IPTAM = "tIPTAM";
    public static final String FIELD_T_IPCURCDE = "tIPCURCDE";
    public static final String FIELD_T_HIACBL = "tHIACBL";
    public static final String FIELD_T_IPCDATE = "tIPCDATE";
    public static final String FIELD_T_IPTD = "tIPTD";
    public static final String FIELD_T_IPTXNARR = "tIPTXNARR";
    public static final String FIELD_FULL_MESSAGE = "fullMessage";


    private static final Logger LOG = LoggerFactory.getLogger(ParseCBSMessage.class);
    private static final Fields EMITTED_FIELDS = new Fields(
            FIELD_SEQNUM,
            FIELD_T_IPTETIME,
            FIELD_T_IPPBR,
            FIELD_T_IPPSTEM,
            FIELD_T_IPTTST,
            FIELD_T_IPTCLCDE,
            FIELD_T_IPTAM,
            FIELD_T_IPCURCDE,
            FIELD_T_HIACBL,
            FIELD_T_IPCDATE,
            FIELD_T_IPTD,
            FIELD_T_IPTXNARR,
            FIELD_FULL_MESSAGE
    );
    private final com.devcycle.explorestorm.util.JSONParser JSONParser = new JSONParser();
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
        Map<String, Object> fieldsMap = new LinkedHashMap<String, Object>();
        JSONObject jsonObject = null;
        try {
            jsonObject = new JSONObject(jsonMessage);
            fieldsMap.put(ParseCBSMessage.FIELD_SEQNUM, JSONParser.parseInt(jsonObject, ParseCBSMessage.FIELD_SEQNUM));
            fieldsMap.put(ParseCBSMessage.FIELD_T_IPTETIME, JSONParser.parseLong(jsonObject, ParseCBSMessage.FIELD_T_IPTETIME));
            fieldsMap.put(ParseCBSMessage.FIELD_T_IPPBR, JSONParser.parseInt(jsonObject, ParseCBSMessage.FIELD_T_IPPBR));
            fieldsMap.put(ParseCBSMessage.FIELD_T_IPPSTEM, JSONParser.parseLong(jsonObject, ParseCBSMessage.FIELD_T_IPPSTEM));
            fieldsMap.put(ParseCBSMessage.FIELD_T_IPTTST, JSONParser.parseInt(jsonObject, ParseCBSMessage.FIELD_T_IPTTST));
            fieldsMap.put(ParseCBSMessage.FIELD_T_IPTCLCDE, JSONParser.parseInt(jsonObject, ParseCBSMessage.FIELD_T_IPTCLCDE));
            fieldsMap.put(ParseCBSMessage.FIELD_T_IPTAM, JSONParser.parseBigDecimal(jsonObject, ParseCBSMessage.FIELD_T_IPTAM));
            fieldsMap.put(ParseCBSMessage.FIELD_T_IPCURCDE, JSONParser.parseInt(jsonObject, ParseCBSMessage.FIELD_T_IPCURCDE));
            fieldsMap.put(ParseCBSMessage.FIELD_T_HIACBL, JSONParser.parseBigDecimal(jsonObject, ParseCBSMessage.FIELD_T_HIACBL));
            fieldsMap.put(ParseCBSMessage.FIELD_T_IPCDATE, JSONParser.parseDateString(jsonObject, ParseCBSMessage.FIELD_T_IPCDATE));
            fieldsMap.put(ParseCBSMessage.FIELD_T_IPTD, JSONParser.parseDateString(jsonObject, ParseCBSMessage.FIELD_T_IPTD));
            fieldsMap.put(ParseCBSMessage.FIELD_T_IPTXNARR, JSONParser.parseString(jsonObject, ParseCBSMessage.FIELD_T_IPTXNARR));
            fieldsMap.put(ParseCBSMessage.FIELD_FULL_MESSAGE, jsonMessage);
        } catch (JSONException e) {
            LOG.error("Error in JSON: " + jsonMessage, e);
        }
        return fieldsMap;
    }
}

