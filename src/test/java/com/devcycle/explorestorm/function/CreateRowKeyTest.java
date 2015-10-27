package com.devcycle.explorestorm.function;

import backtype.storm.tuple.Values;
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
        CreateRowKey transform = new CreateRowKey();
        TridentTuple tuple = mock(TridentTuple.class);
        Mockito.when(tuple.getLongByField(ParseCBSMessage.FIELD_SEQNUM)).thenReturn(123L);
        Mockito.when(tuple.getStringByField(ParseCBSMessage.FIELD_T_IPTD)).thenReturn("2015-10-27");
        TridentCollector collector = mock(TridentCollector.class);
        transform.execute(tuple, collector);

        // assert collector in correct state
        verify(collector).emit(any(List.class));
        String expectedKey = "123-2015-10-27";
        Values expectedValues = new Values(expectedKey);
        verify(collector).emit(expectedValues);
    }

}