package com.devcycle.explorestorm.function;

import backtype.storm.tuple.Fields;
import com.devcycle.explorestorm.scheme.ExploreScheme;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.Map;

/**
 * Created by chrishowe-jones on 30/09/15.
 */
public class ExploreLogFunction extends Fields implements Function {

    private static final Logger LOG = LoggerFactory.getLogger(ExploreLogFunction.class);

    public void execute(TridentTuple tuple, TridentCollector collector) {
        LOG.info("******MY MESSAGE ****** " + ExploreScheme.prettyPrintTuple(tuple));
    }

    public void prepare(Map conf, TridentOperationContext context) {
        // nothing to do
    }

    public void cleanup() {
        // nothing to do
    }
}
