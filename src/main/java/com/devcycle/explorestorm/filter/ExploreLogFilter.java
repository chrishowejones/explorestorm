package com.devcycle.explorestorm.filter;

import com.devcycle.explorestorm.scheme.ExploreScheme;
import org.datanucleus.util.Log4JLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.Filter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.Map;

/**
 * Created by chrishowe-jones on 30/09/15.
 */
public class ExploreLogFilter implements Filter {


    private static final Logger LOG = LoggerFactory.getLogger(ExploreLogFilter.class);

    public boolean isKeep(TridentTuple tuple) {
        LOG.info("******MY MESSAGE ****** " + ExploreScheme.prettyPrintTuple(tuple));
        return true;
    }

    public void prepare(Map conf, TridentOperationContext context) {
        // do nothing
    }

    public void cleanup() {
        // do nothing
    }
}
