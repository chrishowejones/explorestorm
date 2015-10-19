package com.devcycle.explorestorm.function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.Map;
import java.util.Random;

/**
 * Created by chrishowe-jones on 15/10/15.
 */
public class PrintFunction extends BaseFunction {

    private static final Logger LOG = LoggerFactory.getLogger(PrintFunction.class);

    private static final Random RANDOM = new Random();

    @Override
    public void execute(TridentTuple tuple, TridentCollector tridentCollector) {
        LOG.info(tuple.toString());
    }
}
