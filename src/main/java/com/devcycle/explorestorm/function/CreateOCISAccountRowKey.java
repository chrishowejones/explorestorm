package com.devcycle.explorestorm.function;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * Created by chris howe-jones on 27/10/15.
 */
public class CreateOCISAccountRowKey extends BaseFunction {

    /**
     * Transform the CBS message to create a rowKey for HBase that is string value of account number.
     *
     * @param tuple
     * @param collector
     */
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Long accountNumber = tuple.getLongByField(CBSMessageFields.FIELD_T_IPPSTEM);

        String rowKey = Long.toString(accountNumber);
        collector.emit(new Values(rowKey));
    }
}
