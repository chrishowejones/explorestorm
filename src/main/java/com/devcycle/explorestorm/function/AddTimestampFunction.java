package com.devcycle.explorestorm.function;


import backtype.storm.tuple.Values;
import org.apache.storm.hbase.common.Utils;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * Created by chrishowe-jones on 01/12/15.
 */
public class AddTimestampFunction extends BaseFunction {

    /**
     * Add timestamp for time now to the field.
     *
     * @param tuple
     * @param collector
     */
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        collector.emit(new Values(System.currentTimeMillis()));
    }
}
