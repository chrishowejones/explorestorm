package com.devcycle.explorestorm.function;

import backtype.storm.tuple.Values;
import com.devcycle.explorestorm.topologies.BalanceAlertTopology;
import com.devcycle.explorestorm.topologies.OCISDetails;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.math.BigDecimal;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by chris howe-jones on 02/11/15.
 */
@RunWith(MockitoJUnitRunner.class)
public class RaiseLowBalanceAlertTest {

    @Mock
    private TridentCollector collector;
    @Mock
    private TridentTuple tuple;

    @Test
    public void testRaiseAlert() {
        // mock tuple to return account number, balance threshold and balance
        BigDecimal balance = new BigDecimal("39.99");
        String threshold = "40";
        long accountNumber = 1234567L;
        String accountNumberString = Long.toString(accountNumber);
        when(tuple.getValueByField(ParseCBSMessage.FIELD_T_HIACBL)).thenReturn(balance);
        when(tuple.getStringByField(OCISDetails.THRESHOLD.getValue())).thenReturn(threshold);
        when(tuple.getLongByField(ParseCBSMessage.FIELD_T_IPPSTEM)).thenReturn(accountNumber);

        RaiseLowBalanceAlert alertFunction = new RaiseLowBalanceAlert();
        alertFunction.execute(tuple, collector);

        ArgumentCaptor<Values> valuesCaptor = ArgumentCaptor.forClass(Values.class);
        verify(collector).emit(valuesCaptor.capture());

        Values values = valuesCaptor.getValue();
        assertThat(values, notNullValue());
        assertThat(values.size(), is(2));

        assertThat((String)values.get(0), is(accountNumberString));
        assertThat((String)values.get(1), containsString("balance is under expected threshold"));
    }

    @Test
    public void testDontRaiseAlertBalanceEqualsThreshold() {
        // mock tuple to return balance threshold and balance
        BigDecimal balance = new BigDecimal("40");
        String threshold = "40";
        when(tuple.getValueByField(ParseCBSMessage.FIELD_T_HIACBL)).thenReturn(balance);
        when(tuple.getStringByField(OCISDetails.THRESHOLD.getValue())).thenReturn(threshold);

        RaiseLowBalanceAlert alertFunction = new RaiseLowBalanceAlert();
        alertFunction.execute(tuple, collector);

        ArgumentCaptor<Values> valuesCaptor = ArgumentCaptor.forClass(Values.class);
        verify(collector).emit(valuesCaptor.capture());

        Values values = valuesCaptor.getValue();
        assertThat(values, notNullValue());
        assertThat(values.size(), is(2));
        assertThat((String)values.get(0), is("0"));
        assertThat(values.get(1), nullValue());
    }

    @Test
    public void testDontRaiseAlertBalanceGreaterThanThreshold() {
        // mock tuple to return balance threshold and balance
        BigDecimal balance = new BigDecimal("40.01");
        String threshold = "40";
        when(tuple.getValueByField(ParseCBSMessage.FIELD_T_HIACBL)).thenReturn(balance);
        when(tuple.getStringByField(OCISDetails.THRESHOLD.getValue())).thenReturn(threshold);

        RaiseLowBalanceAlert alertFunction = new RaiseLowBalanceAlert();
        alertFunction.execute(tuple, collector);

        ArgumentCaptor<Values> valuesCaptor = ArgumentCaptor.forClass(Values.class);
        verify(collector).emit(valuesCaptor.capture());

        Values values = valuesCaptor.getValue();
        assertThat(values, notNullValue());
        assertThat(values.size(), is(2));
        assertThat((String)values.get(0), is("0"));
        assertThat(values.get(1), nullValue());
    }

}