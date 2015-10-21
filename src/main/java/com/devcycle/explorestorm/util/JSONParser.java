package com.devcycle.explorestorm.util;

import com.devcycle.explorestorm.function.ParseCBSMessage;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.MathContext;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.LinkedHashMap;
import java.util.Map;

public class JSONParser implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(JSONParser.class);

    public String parseString(JSONObject json, String key) throws JSONException {
        String value = null;
        if (json.has(key) && !json.isNull(key)) {
            value = json.getString(key);
        }
        return value;
    }

    public String parseDateString(JSONObject json, String key) throws JSONException {
        String dateString = null;
        if (json.has(key) && !json.isNull(key)) {
            String yymmdd = json.getString(key);
            SimpleDateFormat inputDateFormat = new SimpleDateFormat("yyMMdd");
            try {
                SimpleDateFormat outputDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                dateString = outputDateFormat.format(inputDateFormat.parse(yymmdd));
            } catch (ParseException e) {
                LOG.error("Can't parse date " + key + ": " + yymmdd, e);
            }
        }

        return dateString;
    }

    public BigDecimal parseBigDecimal(JSONObject json, String key) throws JSONException {
        BigDecimal bigDecimal = null;
        if (json.has(key) && !json.isNull(key)) {
            String numberStringValue = json.getString(key).trim();
            String bigDecimalString = buildBigDecimalString(numberStringValue);
            bigDecimal = new BigDecimal(bigDecimalString, new MathContext(2));
        }
        return bigDecimal;
    }

    public String buildBigDecimalString(String numberStringValue) {
        String doubleStringValue = "0.00";
        switch (numberStringValue.length()) {
            case 0:
            case 1:
                // add 0.0 to start
                doubleStringValue = "0.0" + numberStringValue;
                break;
            case 2:
                // add decimal point at start
                doubleStringValue = "0." + numberStringValue;
                break;
            default:
                // add decimal point at position 2 from end of string
                doubleStringValue = numberStringValue.substring(0, numberStringValue.length() - 2) + "." + numberStringValue.substring(numberStringValue.length() - 2);
        }
        return doubleStringValue;
    }

    public Long parseLong(JSONObject json, String key) throws JSONException {
        Long returnLong = null;
        if (json.has(key) && !json.isNull(key))
            returnLong = json.getLong(key);
        return returnLong;
    }

    public Integer parseInt(JSONObject json, String key) throws JSONException {
        Integer returnInt = null;
        if (json.has(key) && !json.isNull(key))
            returnInt = json.getInt(key);
        return returnInt;
    }
}