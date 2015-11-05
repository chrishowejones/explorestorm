package com.devcycle.explorestorm.scheme;

import org.json.JSONObject;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

public class CBSMessageFields implements Serializable {

    public static final String FIELD_SEQNUM = "SEQNUM"; // Sequence number
    public static final String FIELD_TIME = "TIME"; // TE Time hh:mm:ss
    public static final String FIELD_BRANCH_CODE = "tIPPBR"; // parent branch code
    public static final String FIELD_PARENT_STEM = "tIPPSTEM"; // parent stem
    public static final String FIELD_TXN_TYPE = "tIPTTST"; // txn type/subtype
    public static final String FIELD_TXN_CODE = "tIPTCLCDE"; // txn class/code
    public static final String FIELD_TXN_AMOUNT = "tIPTAM"; // transaction amount
    public static final String FIELD_CURRENCY_CDE = "tIPCURCDE"; // currency code
    public static final String FIELD_CURRENT_ACCOUNT_BALANCE = "tHIACBL"; // account balance
    public static final String FIELD_CURRENT_DATE = "tIPCDATE"; // current date
    public static final String FIELD_TXN_DATE = "tIPTD"; // transaction date
    public static final String FIELD_TXN_NARRATIVE = "tIPTXNARR"; // narrative
    public static final String FIELD_FULL_MESSAGE = "fullMessage"; // full message
    public static final String FIELD_TXN_CLASS = "tIPTCLASS"; // transaction class
    public static final String FIELD_ACCOUNT_NUMBER = "ACCNUM"; // account number including sort code etc.

    private static Map<String, Class> types = new HashMap<String, Class>() {
        {
            put(FIELD_SEQNUM, Long.class);
            put(FIELD_TIME, String.class);
            put(FIELD_TXN_TYPE, Integer.class);
            put(FIELD_TXN_CODE, Integer.class);
            put(FIELD_ACCOUNT_NUMBER, Long.class);
            put(FIELD_TXN_AMOUNT, BigDecimal.class);
            put(FIELD_CURRENCY_CDE, Integer.class);
            put(FIELD_CURRENT_ACCOUNT_BALANCE, BigDecimal.class);
            put(FIELD_CURRENT_DATE, String.class);
            put(FIELD_TXN_DATE, String.class);
            put(FIELD_TXN_NARRATIVE, String.class);
            put(FIELD_FULL_MESSAGE, JSONObject.class);
            put(FIELD_TXN_CLASS, Integer.class);
            put(FIELD_BRANCH_CODE, Integer.class);
            put(FIELD_PARENT_STEM, Long.class);
        }
    };

    public Class getType(String fieldName) {
        return types.get(fieldName);
    }
}