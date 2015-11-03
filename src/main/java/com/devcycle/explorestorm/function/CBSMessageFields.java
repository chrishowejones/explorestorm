package com.devcycle.explorestorm.function;

import java.io.Serializable;

public interface CBSMessageFields  {
    String FIELD_SEQNUM = "SEQNUM"; // Sequence number
    String FIELD_T_IPTETIME = "tIPTETIME"; // TE Time HHMMSS
    String FIELD_T_IPPBR = "tIPPBR"; // parent branch code
    String FIELD_T_IPPSTEM = "tIPPSTEM"; // parent stem
    String FIELD_T_IPTTST = "tIPTTST"; // txn type/subtype
    String FIELD_T_IPTCLCDE = "tIPTCLCDE"; // txn class/code
    String FIELD_T_IPTAM = "tIPTAM"; // transaction amount
    String FIELD_T_IPCURCDE = "tIPCURCDE"; // currency code
    String FIELD_T_HIACBL = "tHIACBL"; // account balance
    String FIELD_T_IPCDATE = "tIPCDATE"; // current date
    String FIELD_T_IPTD = "tIPTD"; // transaction date
    String FIELD_T_IPTXNARR = "tIPTXNARR"; // narrative
    String FIELD_FULL_MESSAGE = "fullMessage"; // full message
    String FIELD_T_IPTCLASS = "tIPTCLASS"; // transaction class
}