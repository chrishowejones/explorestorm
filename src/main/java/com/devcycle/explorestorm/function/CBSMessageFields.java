package com.devcycle.explorestorm.function;

import java.io.Serializable;

public interface CBSMessageFields  {
    String FIELD_SEQNUM = "SEQNUM"; // Sequence number
    String FIELD_TIME = "TIME"; // TE Time hh:mm:ss
    String FIELD_BRANCH_CODE = "tIPPBR"; // parent branch code
    String FIELD_PARENT_STEM = "tIPPSTEM"; // parent stem
    String FIELD_TXN_TYPE = "tIPTTST"; // txn type/subtype
    String FIELD_TXN_CODE = "tIPTCLCDE"; // txn class/code
    String FIELD_TXN_AMOUNT = "tIPTAM"; // transaction amount
    String FIELD_CURRENCY_CDE = "tIPCURCDE"; // currency code
    String FIELD_CURRENT_ACCOUNT_BALANCE = "tHIACBL"; // account balance
    String FIELD_CURRENT_DATE = "tIPCDATE"; // current date
    String FIELD_TXN_DATE = "tIPTD"; // transaction date
    String FIELD_TXN_NARRATIVE = "tIPTXNARR"; // narrative
    String FIELD_FULL_MESSAGE = "fullMessage"; // full message
    String FIELD_TXN_CLASS = "tIPTCLASS"; // transaction class
    String FIELD_ACCOUNT_NUMBER = "ACCNUM"; // account number including sort code etc.
}