package com.devcycle.explorestorm.function;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * Created by chris howe-jones on 27/10/15.
 */
public class CreateRowKey extends BaseFunction {

    /**
     * Transform the CBS message to create a rowKey for HBase that is composite key of
     * the sortcode, account number and the transaction date.
     *
     * @param tuple
     * @param collector
     */
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Long accountNumber = tuple.getLongByField(ParseCBSMessage.FIELD_T_IPPSTEM);
        String transactionDate = tuple.getStringByField(ParseCBSMessage.FIELD_T_IPTD);
        String rowKey = Long.toString(accountNumber) + "-" + transactionDate;
        collector.emit(new Values(rowKey));
    }
}
