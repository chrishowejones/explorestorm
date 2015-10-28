package com.devcycle.explorestorm.filter;

import com.devcycle.explorestorm.function.ParseCBSMessage;
import storm.trident.operation.Filter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.Map;

/**
 * Created by chrishowe-jones on 26/10/15.
 */
public class RemoveInvalidMessages implements Filter {

    private final String transactionIdField;
    private final String[] keyFields;

    public RemoveInvalidMessages(String transactionIdField, String[] keyFields) {
        this.transactionIdField = transactionIdField;
        this.keyFields = keyFields;
    }

    @Override
    public boolean isKeep(TridentTuple tuple) {
        return validMessage(tuple);
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {

    }

    @Override
    public void cleanup() {

    }

    private boolean validMessage(TridentTuple tuple) {
        return validTuple(tuple) && validFormat(tuple);
    }

    private boolean validFormat(TridentTuple tuple) {
        return isValid(tuple);
    }

    private boolean isValid(TridentTuple tuple) {
        boolean valid = true;
        valid = hasFields(tuple) && hasKeyFields(tuple) && hasTransactionId(tuple);
        return valid;
    }

    private boolean hasTransactionId(TridentTuple tuple) {
        return tuple.getValueByField(transactionIdField) != null;
    }

    private boolean hasKeyFields(TridentTuple tuple) {
        for (String keyField : keyFields) {
            if (tuple.getValueByField(keyField) == null)
                return false;
        }
        return true;
    }

    private boolean hasFields(TridentTuple tuple) {
        return tuple.getFields() != null && tuple.getFields().size() > 0 && !tuple.isEmpty();
    }

    private boolean validTuple(TridentTuple tuple) {
        return tuple != null && !tuple.isEmpty();
    }
}
