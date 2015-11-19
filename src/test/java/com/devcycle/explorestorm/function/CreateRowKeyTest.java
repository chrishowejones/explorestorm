package com.devcycle.explorestorm.function;

import backtype.storm.tuple.Values;
import com.devcycle.explorestorm.scheme.CBSMessageFields;
import org.junit.Test;
import org.mockito.Mockito;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.List;

import static org.mockito.Matchers.any;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Created by chris howe-jones on 27/10/15.
 */
public class CreateRowKeyTest {

    @Test
    public void testExecute() {
        CreateAccountTxnRowKey transform = new CreateAccountTxnRowKey();
        TridentTuple tuple = mock(TridentTuple.class);
        Mockito.when(tuple.getLongByField(CBSMessageFields.FIELD_ACCOUNT_NUMBER)).thenReturn(1234567890L);
        Mockito.when(tuple.getStringByField(CBSMessageFields.FIELD_TXN_DATE)).thenReturn("2015-10-27");
        TridentCollector collector = mock(TridentCollector.class);
        transform.execute(tuple, collector);

        // assert collector in correct state
        //noinspection unchecked
        verify(collector).emit(any(List.class));
        String expectedKey = "1234567890-2015-10-27";
        Values expectedValues = new Values(expectedKey);
        verify(collector).emit(expectedValues);
    }

}