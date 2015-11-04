package com.devcycle.explorestorm.function;

import backtype.storm.tuple.Values;
import com.devcycle.explorestorm.topologies.OCISDetails;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashSet;

/**
 * Created by chris howe-jones on 02/11/15.
 */
public class RaiseLowBalanceAlert extends BaseFunction {

    private static final Logger LOG = LoggerFactory.getLogger(RaiseLowBalanceAlert.class);
    private static final Integer[] VALID_TXN_CLASS_ARRAY = {6, 10, 13, 17, 20};
    private static final HashSet<Integer> VALID_TRANSACTION_CLASSES = new HashSet<>(Arrays.asList(VALID_TXN_CLASS_ARRAY));
    private static final Integer[] DEBIT_SO_TYPES_ARRAY = {2269, 2270, 2503, 2505, 3037, 3039};
    private static final HashSet<Integer> DEBIT_STANDING_ORDER_TYPES = new HashSet<>(Arrays.asList(DEBIT_SO_TYPES_ARRAY));

    /**
     * Raises a low balance alert
     *
     * @param tuple
     * @param collector
     */
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        final BigDecimal currentBalance = (BigDecimal)tuple.getValueByField(CBSMessageFields.FIELD_CURRENT_ACCOUNT_BALANCE);
        LOG.trace("CurrentBalance = " + currentBalance.toString());
        final String thresholdString = tuple.getStringByField(OCISDetails.THRESHOLD);
        final long accountNumber = tuple.getLongByField(CBSMessageFields.FIELD_ACCOUNT_NUMBER);
        final String accountNumberStr = Long.toString(accountNumber);
        final BigDecimal transactionAmount = (BigDecimal)tuple.getValueByField(CBSMessageFields.FIELD_TXN_AMOUNT);
        final int transactionClass = tuple.getIntegerByField(CBSMessageFields.FIELD_TXN_CLASS);
        final int transactionType = tuple.getIntegerByField(CBSMessageFields.FIELD_TXN_TYPE);
        final boolean isDebit = isDebit(transactionClass, transactionType);
        Values alert = raiseAlert(isDebit, currentBalance, thresholdString, accountNumberStr, transactionAmount);
        collector.emit(alert);
    }

    boolean isDebit(int transactionClass, int transactionType) {
        return VALID_TRANSACTION_CLASSES.contains(transactionClass) ||
                isStandingOrderDebit(transactionClass, transactionType);
    }

    boolean isStandingOrderDebit(int transactionClass, int transactionType) {
        return transactionClass == 15 && DEBIT_STANDING_ORDER_TYPES.contains(transactionType);
    }

    private Values raiseAlert(boolean isDebit, BigDecimal currentBalance, String thresholdString, String accountNumberStr, BigDecimal transactionAmount) {
        Values alert = new Values();
        alert.add(accountNumberStr);
        final BigDecimal threshold = new BigDecimal(thresholdString);
        if (isDebit && currentBalance.compareTo(threshold) < 0
                && amountTippedBalance(transactionAmount, currentBalance, threshold)) {
            final String alertMessage = accountNumberStr + "|balance is under expected threshold|" + currentBalance;
            alert.add(alertMessage);
        } else {
            alert.add(null);
        }
        return alert;
    }

    private boolean amountTippedBalance(BigDecimal transactionAmount, BigDecimal currentBalance, BigDecimal threshold) {
        return currentBalance.add(transactionAmount).compareTo(threshold) >= 0;
    }
}
