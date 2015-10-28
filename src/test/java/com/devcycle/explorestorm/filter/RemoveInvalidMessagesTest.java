package com.devcycle.explorestorm.filter;

import backtype.storm.tuple.Fields;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import storm.trident.tuple.TridentTuple;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

/**
 * Created by chrishowe-jones on 28/10/15.
 */
@RunWith(MockitoJUnitRunner.class)
public class RemoveInvalidMessagesTest {

    @Mock
    private TridentTuple tuple;

    @Test
    public void testValidMessage() {
        final String seqnum = "SEQNUM";
        final String tIPPBR = "tIPPBR";
        final String tIPPSTEM = "tIPPSTEM";
        final String tIPTD = "tIPTD";
        Fields fields = new Fields( seqnum, tIPPBR, tIPPSTEM, tIPTD);
        when(tuple.getFields()).thenReturn(fields);
        when(tuple.getValueByField(seqnum)).thenReturn(123);
        when(tuple.getValueByField(tIPPBR)).thenReturn(556677);
        when(tuple.getValueByField(tIPPSTEM)).thenReturn(123456789L);
        when(tuple.getValueByField(tIPTD)).thenReturn("2015-10-27");
        when(tuple.size()).thenReturn(4);

        RemoveInvalidMessages filter = new RemoveInvalidMessages(seqnum, new String[] { tIPPBR, tIPPSTEM, tIPTD });
        assertThat(filter.isKeep(tuple), is(true));
    }

    @Test
    public void testValidMessage2() {
        final String txnid = "txnident";
        final String keyfield = "key field";
        Fields fields = new Fields( txnid, keyfield);
        when(tuple.getFields()).thenReturn(fields);
        when(tuple.getValueByField(txnid)).thenReturn(123);
        when(tuple.getValueByField(keyfield)).thenReturn(556677);
        when(tuple.size()).thenReturn(2);

        RemoveInvalidMessages filter = new RemoveInvalidMessages(txnid, new String[] { keyfield });
        assertThat(filter.isKeep(tuple), is(true));
    }

    @Test
    public void testEmptyTuple() {
        final String txnid = "txnident";
        final String keyfield = "key field";
        Fields fields = new Fields( txnid, keyfield);
        when(tuple.getFields()).thenReturn(fields);
        when(tuple.isEmpty()).thenReturn(true);
        when(tuple.getValueByField(txnid)).thenReturn(123);
        when(tuple.getValueByField(keyfield)).thenReturn(556677);
        when(tuple.size()).thenReturn(2);

        RemoveInvalidMessages filter = new RemoveInvalidMessages(txnid, new String[] { keyfield });
        assertThat(filter.isKeep(tuple), is(false));
    }

    @Test
    public void testInvalidMessageNoSeqNum() {
        final String seqnum = "SEQNUM";
        final String tIPPBR = "tIPPBR";
        final String tIPPSTEM = "tIPPSTEM";
        final String tIPTD = "tIPTD";
        Fields fields = new Fields(tIPPBR, tIPPSTEM, tIPTD);
        when(tuple.getFields()).thenReturn(fields);
        when(tuple.getValueByField(tIPPBR)).thenReturn(556677);
        when(tuple.getValueByField(tIPPSTEM)).thenReturn(123456789L);
        when(tuple.getValueByField(tIPTD)).thenReturn("2015-10-27");
        when(tuple.size()).thenReturn(4);

        RemoveInvalidMessages filter = new RemoveInvalidMessages(seqnum, new String[] { tIPPBR, tIPPSTEM, tIPTD });
        assertThat(filter.isKeep(tuple), is(false));
    }

    @Test
    public void testInvalidMessageNullSeqNum() {
        final String seqnum = "SEQNUM";
        final String tIPPBR = "tIPPBR";
        final String tIPPSTEM = "tIPPSTEM";
        final String tIPTD = "tIPTD";
        Fields fields = new Fields(tIPPBR, tIPPSTEM, tIPTD);
        when(tuple.getFields()).thenReturn(fields);
        when(tuple.getValueByField(seqnum)).thenReturn(null);
        when(tuple.getValueByField(tIPPBR)).thenReturn(556677);
        when(tuple.getValueByField(tIPPSTEM)).thenReturn(123456789L);
        when(tuple.getValueByField(tIPTD)).thenReturn("2015-10-27");
        when(tuple.size()).thenReturn(4);

        RemoveInvalidMessages filter = new RemoveInvalidMessages(seqnum, new String[] { tIPPBR, tIPPSTEM, tIPTD });
        assertThat(filter.isKeep(tuple), is(false));
    }

    @Test
    public void testInvalidMessageMissingtIPPBR() {
        final String seqnum = "SEQNUM";
        final String tIPPSTEM = "tIPPSTEM";
        final String tIPTD = "tIPTD";
        final String tIPPBR = "tIPPBR";
        Fields fields = new Fields( seqnum, tIPPSTEM, tIPTD);
        when(tuple.getFields()).thenReturn(fields);
        when(tuple.getValueByField(seqnum)).thenReturn(123);
        when(tuple.getValueByField(tIPPSTEM)).thenReturn(123456789L);
        when(tuple.getValueByField(tIPTD)).thenReturn("2015-10-27");
        when(tuple.size()).thenReturn(4);

        RemoveInvalidMessages filter = new RemoveInvalidMessages(seqnum, new String[] { tIPPBR, tIPPSTEM, tIPTD });
        assertThat(filter.isKeep(tuple), is(false));
    }

    @Test
    public void testInvalidMessageNulltIPPBR() {
        final String seqnum = "SEQNUM";
        final String tIPPBR = "tIPPBR";
        final String tIPPSTEM = "tIPPSTEM";
        final String tIPTD = "tIPTD";
        Fields fields = new Fields( seqnum, tIPPBR, tIPPSTEM, tIPTD);
        when(tuple.getFields()).thenReturn(fields);
        when(tuple.getValueByField(seqnum)).thenReturn(123);
        when(tuple.getValueByField(tIPPBR)).thenReturn(null);
        when(tuple.getValueByField(tIPPSTEM)).thenReturn(123456789L);
        when(tuple.getValueByField(tIPTD)).thenReturn("2015-10-27");
        when(tuple.size()).thenReturn(4);

        RemoveInvalidMessages filter = new RemoveInvalidMessages(seqnum, new String[] { tIPPBR, tIPPSTEM, tIPTD });
        assertThat(filter.isKeep(tuple), is(false));
    }

    @Test
    public void testInvalidMessageMissingtIPPSTEM() {
        final String seqnum = "SEQNUM";
        final String tIPPBR = "tIPPBR";
        final String tIPPSTEM = "tIPPSTEM";
        final String tIPTD = "tIPTD";
        Fields fields = new Fields( seqnum, tIPPBR, tIPTD);
        when(tuple.getFields()).thenReturn(fields);
        when(tuple.getValueByField(seqnum)).thenReturn(123);
        when(tuple.getValueByField(tIPPBR)).thenReturn(556677);
        when(tuple.getValueByField(tIPTD)).thenReturn("2015-10-27");
        when(tuple.size()).thenReturn(4);

        RemoveInvalidMessages filter = new RemoveInvalidMessages(seqnum, new String[] { tIPPBR, tIPPSTEM, tIPTD });
        assertThat(filter.isKeep(tuple), is(false));
    }

    @Test
    public void testInvalidMessageNulltIPPSTEM() {
        final String seqnum = "SEQNUM";
        final String tIPPBR = "tIPPBR";
        final String tIPPSTEM = "tIPPSTEM";
        final String tIPTD = "tIPTD";
        Fields fields = new Fields( seqnum, tIPPBR, tIPPSTEM, tIPTD);
        when(tuple.getFields()).thenReturn(fields);
        when(tuple.getValueByField(seqnum)).thenReturn(123);
        when(tuple.getValueByField(tIPPBR)).thenReturn(556677);
        when(tuple.getValueByField(tIPPSTEM)).thenReturn(null);
        when(tuple.getValueByField(tIPTD)).thenReturn("2015-10-27");
        when(tuple.size()).thenReturn(4);

        RemoveInvalidMessages filter = new RemoveInvalidMessages(seqnum, new String[] { tIPPBR, tIPPSTEM, tIPTD });
        assertThat(filter.isKeep(tuple), is(false));
    }

    @Test
    public void testInvalidMessageMissingtIPTD() {
        final String seqnum = "SEQNUM";
        final String tIPPBR = "tIPPBR";
        final String tIPPSTEM = "tIPPSTEM";
        final String tIPTD = "tIPTD";
        Fields fields = new Fields( seqnum, tIPPBR, tIPPSTEM);
        when(tuple.getFields()).thenReturn(fields);
        when(tuple.getValueByField(seqnum)).thenReturn(123);
        when(tuple.getValueByField(tIPPBR)).thenReturn(556677);
        when(tuple.getValueByField(tIPPSTEM)).thenReturn(123456789L);
        when(tuple.size()).thenReturn(4);

        RemoveInvalidMessages filter = new RemoveInvalidMessages(seqnum, new String[] { tIPPBR, tIPPSTEM, tIPTD });
        assertThat(filter.isKeep(tuple), is(false));
    }

    @Test
    public void testInvalidMessageNulltIPTD() {
        final String seqnum = "SEQNUM";
        final String tIPPBR = "tIPPBR";
        final String tIPPSTEM = "tIPPSTEM";
        final String tIPTD = "tIPTD";
        Fields fields = new Fields( seqnum, tIPPBR, tIPPSTEM, tIPTD);
        when(tuple.getFields()).thenReturn(fields);
        when(tuple.getValueByField(seqnum)).thenReturn(123);
        when(tuple.getValueByField(tIPPBR)).thenReturn(556677);
        when(tuple.getValueByField(tIPPSTEM)).thenReturn(123456789L);
        when(tuple.getValueByField(tIPTD)).thenReturn(null);
        when(tuple.size()).thenReturn(4);

        RemoveInvalidMessages filter = new RemoveInvalidMessages(seqnum, new String[] { tIPPBR, tIPPSTEM, tIPTD });
        assertThat(filter.isKeep(tuple), is(false));
    }

    @Test
    public void testInvalidMessageMissingTransactionId() {
        final String txnid = "txnid";
        final String tIPPBR = "tIPPBR";
        final String tIPPSTEM = "tIPPSTEM";
        final String tIPTD = "tIPTD";
        Fields fields = new Fields( txnid, tIPPBR, tIPPSTEM, tIPTD);
        when(tuple.getFields()).thenReturn(fields);
        when(tuple.getValueByField("SEQNUM")).thenReturn(123);
        when(tuple.getValueByField(tIPPBR)).thenReturn(556677);
        when(tuple.getValueByField(tIPPSTEM)).thenReturn(123456789L);
        when(tuple.getValueByField(tIPTD)).thenReturn("2015-10-01");
        when(tuple.size()).thenReturn(4);

        RemoveInvalidMessages filter = new RemoveInvalidMessages(txnid, new String[] { tIPPBR, tIPPSTEM, tIPTD });
        assertThat(filter.isKeep(tuple), is(false));
    }

    @Test
    public void testInvalidMessageNullTransactionId() {
        final String txnid = "txnid";
        final String tIPPBR = "tIPPBR";
        final String tIPPSTEM = "tIPPSTEM";
        final String tIPTD = "tIPTD";
        Fields fields = new Fields( txnid, tIPPBR, tIPPSTEM, tIPTD);
        when(tuple.getFields()).thenReturn(fields);
        when(tuple.getValueByField("SEQNUM")).thenReturn(123);
        when(tuple.getValueByField(txnid)).thenReturn(null);
        when(tuple.getValueByField(tIPPBR)).thenReturn(556677);
        when(tuple.getValueByField(tIPPSTEM)).thenReturn(123456789L);
        when(tuple.getValueByField(tIPTD)).thenReturn("2015-10-01");
        when(tuple.size()).thenReturn(4);

        RemoveInvalidMessages filter = new RemoveInvalidMessages(txnid, new String[] { tIPPBR, tIPPSTEM, tIPTD });
        assertThat(filter.isKeep(tuple), is(false));
    }

    @Test
    public void testInvalidMessageMissingKeyField() {
        final String txnid = "txnid";
        final String keyfield1 = "keyfield1";
        final String tIPPSTEM = "tIPPSTEM";
        final String tIPTD = "tIPTD";
        Fields fields = new Fields( txnid, keyfield1, tIPPSTEM);
        when(tuple.getFields()).thenReturn(fields);
        when(tuple.getValueByField(txnid)).thenReturn(123);
        when(tuple.getValueByField("tIPPBR")).thenReturn(556677);
        when(tuple.getValueByField(tIPPSTEM)).thenReturn(123456789L);
        when(tuple.getValueByField(tIPTD)).thenReturn("2015-10-01");
        when(tuple.size()).thenReturn(4);


        RemoveInvalidMessages filter = new RemoveInvalidMessages(txnid, new String[] { keyfield1, tIPPSTEM });
        assertThat(filter.isKeep(tuple), is(false));
    }

    @Test
    public void testInvalidMessageNullKeyField() {
        final String txnid = "txnid";
        final String keyfield1 = "keyfield1";
        final String tIPPSTEM = "tIPPSTEM";
        final String tIPTD = "tIPTD";
        Fields fields = new Fields( txnid, keyfield1, tIPPSTEM);
        when(tuple.getFields()).thenReturn(fields);
        when(tuple.getValueByField(txnid)).thenReturn(123);
        when(tuple.getValueByField(keyfield1)).thenReturn(null);
        when(tuple.getValueByField("tIPPBR")).thenReturn(556677);
        when(tuple.getValueByField(tIPPSTEM)).thenReturn(123456789L);
        when(tuple.getValueByField(tIPTD)).thenReturn("2015-10-01");
        when(tuple.size()).thenReturn(4);


        RemoveInvalidMessages filter = new RemoveInvalidMessages(txnid, new String[] { keyfield1, tIPPSTEM });
        assertThat(filter.isKeep(tuple), is(false));
    }
}