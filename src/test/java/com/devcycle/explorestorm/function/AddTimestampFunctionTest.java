package com.devcycle.explorestorm.function;

import backtype.storm.tuple.Values;
import com.devcycle.explorestorm.scheme.CBSMessageFields;
import org.apache.storm.hbase.common.Utils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.Date;
import java.util.GregorianCalendar;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by chrishowe-jones on 01/12/15.
 */
@RunWith(MockitoJUnitRunner.class)
public class AddTimestampFunctionTest {

    @Mock
    private TridentCollector collector;

    @Mock
    private TridentTuple tuple;

    @Test
    public void testAddTimestamp() {
        AddTimestampFunction addTimestampFunction = new AddTimestampFunction();
        final long timestamp = new GregorianCalendar(2015, 0, 1).getTimeInMillis();
        when(tuple.getLongByField(CBSMessageFields.FIELD_MSG_TIMESTAMP)).thenReturn(timestamp);

        // verify emit arguments
        ArgumentCaptor<Values> valuesArgument = ArgumentCaptor.forClass(Values.class);
        long before = System.currentTimeMillis();

        addTimestampFunction.execute(tuple, collector);

        verify(collector).emit(valuesArgument.capture());

        long after = System.currentTimeMillis();

        // assert values emitted
        Values valuesEmitted = valuesArgument.getValue();
        assertThat(valuesEmitted, notNullValue());
        assertThat(valuesEmitted.size(), is(1));
        Long combinedTimeStamp = (Long) valuesEmitted.get(0);
        assertThat(combinedTimeStamp, greaterThan(0L));
        assertThat(combinedTimeStamp, greaterThanOrEqualTo(before));
        assertThat(combinedTimeStamp, lessThanOrEqualTo(after));
    }

}