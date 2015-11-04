package com.devcycle.explorestorm.function;

import backtype.storm.tuple.Values;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by chris howe-jones on 04/11/15.
 */
@RunWith(MockitoJUnitRunner.class)
public class CreateOCISAccountRowKeyTest {

    @Mock
    private TridentCollector collector;
    @Mock
    private TridentTuple tuple;
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void testExecute() {
        CreateOCISAccountRowKey createOCISAccountRowKey = new CreateOCISAccountRowKey();
        final long accountNumber = 123456789L;
        when(tuple.getLongByField(CBSMessageFields.FIELD_ACCOUNT_NUMBER)).thenReturn(accountNumber);


        // verify emit arguments
        ArgumentCaptor<Values> valuesArgument = ArgumentCaptor.forClass(Values.class);

        createOCISAccountRowKey.execute(tuple, collector);

        verify(collector).emit(valuesArgument.capture());

        // assert values emitted
        Values valuesEmitted = valuesArgument.getValue();
        assertThat(valuesEmitted, notNullValue());
        assertThat(valuesEmitted.size(), is(1));
        assertThat(valuesEmitted.contains(Long.toString(accountNumber)), is(true));
    }

}