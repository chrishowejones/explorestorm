package com.devcycle.explorestorm.function;

import backtype.storm.tuple.Values;
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
        BigDecimal balance = new BigDecimal("39.99").setScale(2);
        String threshold = "40";
        long accountNumber = 1234567L;
        Integer transactionClass = 6;
        Integer transactionType = 2269;
        BigDecimal transactionAmount = new BigDecimal("10.00").setScale(2);

        String accountNumberString = Long.toString(accountNumber);
        when(tuple.getValueByField(CBSMessageFields.FIELD_CURRENT_ACCOUNT_BALANCE)).thenReturn(balance);
        when(tuple.getStringByField(OCISDetails.THRESHOLD)).thenReturn(threshold);
        when(tuple.getLongByField(CBSMessageFields.FIELD_ACCOUNT_NUMBER)).thenReturn(accountNumber);
        when(tuple.getValueByField(CBSMessageFields.FIELD_TXN_AMOUNT)).thenReturn(transactionAmount);
        when(tuple.getIntegerByField(CBSMessageFields.FIELD_TXN_CLASS)).thenReturn(transactionClass);
        when(tuple.getIntegerByField(CBSMessageFields.FIELD_TXN_TYPE)).thenReturn(transactionType);

        RaiseLowBalanceAlert alertFunction = new RaiseLowBalanceAlert();
        alertFunction.execute(tuple, collector);

        ArgumentCaptor<Values> valuesCaptor = ArgumentCaptor.forClass(Values.class);
        verify(collector).emit(valuesCaptor.capture());

        Values values = valuesCaptor.getValue();
        assertThat(values, notNullValue());
        assertThat(values.size(), is(2));

        assertThat((String)values.get(0), is(accountNumberString));
        assertThat((String)values.get(1), containsString("1234567|balance is under expected threshold|39.99"));
    }

    @Test
    public void testDontRaiseAlertAmountDidntTipBalance() {
        // mock tuple to return account number, balance threshold and balance
        BigDecimal balance = new BigDecimal("3.99").setScale(2);
        String threshold = "40";
        long accountNumber = 1234567L;
        Integer transactionClass = 6;
        Integer transactionType = 2269;
        BigDecimal transactionAmount = new BigDecimal("10.00").setScale(2);

        String accountNumberString = Long.toString(accountNumber);
        when(tuple.getValueByField(CBSMessageFields.FIELD_CURRENT_ACCOUNT_BALANCE)).thenReturn(balance);
        when(tuple.getStringByField(OCISDetails.THRESHOLD)).thenReturn(threshold);
        when(tuple.getLongByField(CBSMessageFields.FIELD_ACCOUNT_NUMBER)).thenReturn(accountNumber);
        when(tuple.getValueByField(CBSMessageFields.FIELD_TXN_AMOUNT)).thenReturn(transactionAmount);
        when(tuple.getIntegerByField(CBSMessageFields.FIELD_TXN_CLASS)).thenReturn(transactionClass);
        when(tuple.getIntegerByField(CBSMessageFields.FIELD_TXN_TYPE)).thenReturn(transactionType);

        RaiseLowBalanceAlert alertFunction = new RaiseLowBalanceAlert();
        alertFunction.execute(tuple, collector);

        ArgumentCaptor<Values> valuesCaptor = ArgumentCaptor.forClass(Values.class);
        verify(collector).emit(valuesCaptor.capture());

        Values values = valuesCaptor.getValue();
        assertThat(values, notNullValue());
        assertThat(values.size(), is(2));
        assertThat((String)values.get(0), is(accountNumberString));
        assertThat(values.get(1), nullValue());
    }

    @Test
    public void testRaiseAlertStandingOrderDebit() {
        // mock tuple to return account number, balance threshold and balance
        BigDecimal balance = new BigDecimal("39.99").setScale(2);
        String threshold = "40";
        long accountNumber = 1234567L;
        Integer transactionClass = 15;
        Integer transactionType = 2269;
        BigDecimal transactionAmount = new BigDecimal("10.00").setScale(2);

        String accountNumberString = Long.toString(accountNumber);
        when(tuple.getValueByField(CBSMessageFields.FIELD_CURRENT_ACCOUNT_BALANCE)).thenReturn(balance);
        when(tuple.getStringByField(OCISDetails.THRESHOLD)).thenReturn(threshold);
        when(tuple.getLongByField(CBSMessageFields.FIELD_ACCOUNT_NUMBER)).thenReturn(accountNumber);
        when(tuple.getValueByField(CBSMessageFields.FIELD_TXN_AMOUNT)).thenReturn(transactionAmount);
        when(tuple.getIntegerByField(CBSMessageFields.FIELD_TXN_CLASS)).thenReturn(transactionClass);
        when(tuple.getIntegerByField(CBSMessageFields.FIELD_TXN_TYPE)).thenReturn(transactionType);

        RaiseLowBalanceAlert alertFunction = new RaiseLowBalanceAlert();
        alertFunction.execute(tuple, collector);

        ArgumentCaptor<Values> valuesCaptor = ArgumentCaptor.forClass(Values.class);
        verify(collector).emit(valuesCaptor.capture());

        Values values = valuesCaptor.getValue();
        assertThat(values, notNullValue());
        assertThat(values.size(), is(2));

        assertThat((String)values.get(0), is(accountNumberString));
        assertThat((String)values.get(1), containsString("1234567|balance is under expected threshold|39.99"));
    }

    @Test
    public void testDontRaiseAlertNotDebitTxnClass() {
        // mock tuple to return account number, balance threshold and balance
        BigDecimal balance = new BigDecimal("39.99").setScale(2);
        String threshold = "40";
        long accountNumber = 1234567L;
        Integer transactionClass = 5;
        Integer transactionType = 2269;

        String accountNumberString = Long.toString(accountNumber);
        when(tuple.getValueByField(CBSMessageFields.FIELD_CURRENT_ACCOUNT_BALANCE)).thenReturn(balance);
        when(tuple.getStringByField(OCISDetails.THRESHOLD)).thenReturn(threshold);
        when(tuple.getLongByField(CBSMessageFields.FIELD_ACCOUNT_NUMBER)).thenReturn(accountNumber);
        when(tuple.getIntegerByField(CBSMessageFields.FIELD_TXN_CLASS)).thenReturn(transactionClass);
        when(tuple.getIntegerByField(CBSMessageFields.FIELD_TXN_TYPE)).thenReturn(transactionType);

        RaiseLowBalanceAlert alertFunction = new RaiseLowBalanceAlert();
        alertFunction.execute(tuple, collector);

        ArgumentCaptor<Values> valuesCaptor = ArgumentCaptor.forClass(Values.class);
        verify(collector).emit(valuesCaptor.capture());

        Values values = valuesCaptor.getValue();
        assertThat(values, notNullValue());
        assertThat(values.size(), is(2));
        assertThat((String)values.get(0), is(accountNumberString));
        assertThat(values.get(1), nullValue());
    }

    @Test
    public void testDontRaiseAlertNotDebitTxnType() {
        // mock tuple to return account number, balance threshold and balance
        BigDecimal balance = new BigDecimal("39.99").setScale(2);
        String threshold = "40";
        long accountNumber = 1234567L;
        Integer transactionClass = 15;
        Integer transactionType = 2268;

        String accountNumberString = Long.toString(accountNumber);
        when(tuple.getValueByField(CBSMessageFields.FIELD_CURRENT_ACCOUNT_BALANCE)).thenReturn(balance);
        when(tuple.getStringByField(OCISDetails.THRESHOLD)).thenReturn(threshold);
        when(tuple.getLongByField(CBSMessageFields.FIELD_ACCOUNT_NUMBER)).thenReturn(accountNumber);
        when(tuple.getIntegerByField(CBSMessageFields.FIELD_TXN_CLASS)).thenReturn(transactionClass);
        when(tuple.getIntegerByField(CBSMessageFields.FIELD_TXN_TYPE)).thenReturn(transactionType);

        RaiseLowBalanceAlert alertFunction = new RaiseLowBalanceAlert();
        alertFunction.execute(tuple, collector);

        ArgumentCaptor<Values> valuesCaptor = ArgumentCaptor.forClass(Values.class);
        verify(collector).emit(valuesCaptor.capture());

        Values values = valuesCaptor.getValue();
        assertThat(values, notNullValue());
        assertThat(values.size(), is(2));
        assertThat((String)values.get(0), is(accountNumberString));
        assertThat(values.get(1), nullValue());
    }

    @Test
    public void testDontRaiseAlertBalanceEqualsThreshold() {
        // mock tuple to return balance threshold and balance
        BigDecimal balance = new BigDecimal("40");
        String threshold = "40";
        long accountNumber = 1234567L;
        String accountNumberString = Long.toString(accountNumber);

        when(tuple.getValueByField(CBSMessageFields.FIELD_CURRENT_ACCOUNT_BALANCE)).thenReturn(balance);
        when(tuple.getStringByField(OCISDetails.THRESHOLD)).thenReturn(threshold);
        when(tuple.getLongByField(CBSMessageFields.FIELD_ACCOUNT_NUMBER)).thenReturn(accountNumber);

        RaiseLowBalanceAlert alertFunction = new RaiseLowBalanceAlert();
        alertFunction.execute(tuple, collector);

        ArgumentCaptor<Values> valuesCaptor = ArgumentCaptor.forClass(Values.class);
        verify(collector).emit(valuesCaptor.capture());

        Values values = valuesCaptor.getValue();
        assertThat(values, notNullValue());
        assertThat(values.size(), is(2));
        assertThat((String)values.get(0), is(accountNumberString));
        assertThat(values.get(1), nullValue());
    }

    @Test
    public void testDontRaiseAlertBalanceGreaterThanThreshold() {
        // mock tuple to return balance threshold and balance
        BigDecimal balance = new BigDecimal("40.01");
        String threshold = "40";
        long accountNumber = 1234567L;
        String accountNumberString = Long.toString(accountNumber);

        when(tuple.getValueByField(CBSMessageFields.FIELD_CURRENT_ACCOUNT_BALANCE)).thenReturn(balance);
        when(tuple.getStringByField(OCISDetails.THRESHOLD)).thenReturn(threshold);
        when(tuple.getLongByField(CBSMessageFields.FIELD_ACCOUNT_NUMBER)).thenReturn(accountNumber);

        RaiseLowBalanceAlert alertFunction = new RaiseLowBalanceAlert();
        alertFunction.execute(tuple, collector);

        ArgumentCaptor<Values> valuesCaptor = ArgumentCaptor.forClass(Values.class);
        verify(collector).emit(valuesCaptor.capture());

        Values values = valuesCaptor.getValue();
        assertThat(values, notNullValue());
        assertThat(values.size(), is(2));
        assertThat((String)values.get(0), is(accountNumberString));
        assertThat(values.get(1), nullValue());
    }

    @Test
    public void testDebitTransactionClasses() {
        RaiseLowBalanceAlert alertFunction = new RaiseLowBalanceAlert();
        int[] debitTransactionClasses = {6, 10,13,17,20};
        for (int txnClass : debitTransactionClasses) {
            assertThat(alertFunction.isDebit(txnClass, 1), is(true));
        }
    }

    @Test
    public void testDebitStandingOrder() {
        RaiseLowBalanceAlert alertFunction = new RaiseLowBalanceAlert();
        int[] debitSOTxnTypes = {2269, 2270, 2503, 2505, 3037, 3039};
        for (int txnType : debitSOTxnTypes) {
            assertThat(alertFunction.isStandingOrderDebit(15, txnType), is(true));
        }
    }
}