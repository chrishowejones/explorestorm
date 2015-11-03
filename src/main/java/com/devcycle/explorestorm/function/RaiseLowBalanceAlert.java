package com.devcycle.explorestorm.function;

import backtype.storm.tuple.Values;
import com.devcycle.explorestorm.topologies.BalanceAlertTopology;
import com.devcycle.explorestorm.topologies.OCISDetails;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.math.BigDecimal;

/**
 * Created by chris howe-jones on 02/11/15.
 */
public class RaiseLowBalanceAlert extends BaseFunction {

    private Logger LOG = LoggerFactory.getLogger(RaiseLowBalanceAlert.class);

    /**
     * Raises a low balance alert
     *
     * @param tuple
     * @param collector
     */
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        BigDecimal currentBalance = (BigDecimal)tuple.getValueByField(ParseCBSMessage.FIELD_T_HIACBL);
        LOG.trace("CurrentBalance = " + currentBalance.toString());
        String thresholdString = tuple.getStringByField(OCISDetails.THRESHOLD.getValue());
        Values alert = new Values();
        long accountNumber = tuple.getLongByField(ParseCBSMessage.FIELD_T_IPPSTEM);
        alert.add(Long.toString(accountNumber));
        if (currentBalance.compareTo(new BigDecimal(thresholdString)) < 0) {
            final String alertMessage = "balance is under expected threshold|" + currentBalance;
            alert.add(alertMessage);
        } else {
            alert.add(null);
        }
        collector.emit(alert);
    }
}
