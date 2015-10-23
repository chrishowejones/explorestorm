package com.devcycle.explorestorm.filter;

import storm.trident.operation.Filter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.Map;

/**
 * Created by chrishowe-jones on 26/10/15.
 */
public class RemoveInvalidMessages implements Filter {

    @Override
    public boolean isKeep(TridentTuple tuple) {
        return validMessage(tuple);
    }

    private boolean validMessage(TridentTuple tuple) {
        return validTuple(tuple) && validFormat(tuple);
    }

    private boolean validFormat(TridentTuple tuple) {
        return tuple.getFields() != null && tuple.getFields().size() > 0;
    }

    private boolean validTuple(TridentTuple tuple) {
        return tuple != null && !tuple.isEmpty();
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {

    }

    @Override
    public void cleanup() {

    }
}
