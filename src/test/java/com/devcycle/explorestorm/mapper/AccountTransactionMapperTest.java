package com.devcycle.explorestorm.mapper;

import org.apache.storm.hbase.common.ColumnList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import storm.trident.tuple.TridentTuple;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by chrishowe-jones on 26/10/15.
 */
public class AccountTransactionMapperTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void testWithRowKeyField() {
        AccountTransactionMapper mapper = new AccountTransactionMapper();
        String rowKeyField = "account-txnDate";
        mapper = mapper.withRowKeyField(rowKeyField);
        assertThat(mapper, notNullValue());
        assertThat(mapper.getRowKeyField(), is(rowKeyField));
    }

    @Test
    public void testRowKey() {
        AccountTransactionMapper mapper = new AccountTransactionMapper();
        String rowKeyField = "account-txnDate";
        mapper = mapper.withRowKeyField(rowKeyField);

        // create tuple and call rowKey
        TridentTuple tuple = mock(TridentTuple.class);
        String expectedValue = "testing";
        byte[] expectedRowKeyBytes = expectedValue.getBytes();
        when(tuple.getValueByField(rowKeyField)).thenReturn(expectedValue);
        assertThat(mapper.rowKey(tuple), equalTo(expectedRowKeyBytes));
    }

    @Test
    public void testRowKeyNullValue() {
        AccountTransactionMapper mapper = new AccountTransactionMapper();
        String rowKeyField = "account-txnDate";
        mapper = mapper.withRowKeyField(rowKeyField);

        // create tuple and call rowKey
        TridentTuple tuple = mock(TridentTuple.class);
        assertThat(mapper.rowKey(tuple), nullValue());
    }

    @Test
    public void testWithColumnFamiliesOneFamily() {
        AccountTransactionMapper mapper = new AccountTransactionMapper();
        String columnFamily = "cf";
        List<String> columnFamilies = new ArrayList<String>();
        columnFamilies.add(columnFamily);

        final AccountTransactionMapper accountTransactionMapper = mapper.withColumnFamilies(columnFamilies);
        assertThat(accountTransactionMapper, notNullValue());
        assertThat(accountTransactionMapper, instanceOf(AccountTransactionMapper.class));
        assertThat(accountTransactionMapper.getColumnFamilies(), hasSize(1));
        assertThat(accountTransactionMapper.getColumnFamilies().get(0), is(columnFamily));
    }

    @Test
    public void testWithColumnFamiliesMultipleFamilies() {
        AccountTransactionMapper mapper = new AccountTransactionMapper();
        String columnFamily = "cf";
        String columnFamily2 = "cf2";
        String columnFamily3 = "cf3";

        List<String> columnFamilies = new ArrayList<>();
        columnFamilies.add(columnFamily);
        columnFamilies.add(columnFamily2);
        columnFamilies.add(columnFamily3);

        AccountTransactionMapper accountTransactionMapper = mapper.withColumnFamilies(columnFamilies);

        assertThat(accountTransactionMapper, notNullValue());
        assertThat(accountTransactionMapper, instanceOf(AccountTransactionMapper.class));
        assertThat(accountTransactionMapper.getColumnFamilies(), hasSize(3));
        assertThat(accountTransactionMapper.getColumnFamilies().get(0), is(columnFamily));
        assertThat(accountTransactionMapper.getColumnFamilies().get(1), is(columnFamily2));
        assertThat(accountTransactionMapper.getColumnFamilies().get(2), is(columnFamily3));
    }

    @Test
    public void testWithOneColumnFieldPrefix() {
        AccountTransactionMapper mapper = new AccountTransactionMapper();
        List<String> columnFieldPrefixes = new ArrayList<String>();
        final String columnFamily = "cf";
        final String seqnum = "seqnum";
        columnFieldPrefixes.add(seqnum);

        List<String> columnFamilies = new ArrayList<>();
        columnFamilies.add(columnFamily);

        mapper.withColumnFamilies(columnFamilies);

        AccountTransactionMapper accountTransactionMapper = mapper.withColumnFieldPrefixes(columnFamily, columnFieldPrefixes);

        assertThat(accountTransactionMapper, notNullValue());
        assertThat(accountTransactionMapper.getColumnFieldPrefixes(columnFamily), notNullValue());
        assertThat(accountTransactionMapper.getColumnFieldPrefixes(columnFamily), hasSize(1));
        assertThat(accountTransactionMapper.getColumnFieldPrefixes(columnFamily).get(0), is(seqnum));
    }

    @Test
    public void testWithMultiColumnFieldPrefixOneFamily() {
        AccountTransactionMapper mapper = new AccountTransactionMapper();
        List<String> columnFieldPrefixes = new ArrayList<String>();
        final String columnFamily = "cf";
        final String seqnum = "seqnum";
        final String txndate = "txndate";
        columnFieldPrefixes.add(seqnum);
        columnFieldPrefixes.add(txndate);

        List<String> columnFamilies = new ArrayList<>();
        columnFamilies.add(columnFamily);
        mapper.withColumnFamilies(columnFamilies);

        AccountTransactionMapper accountTransactionMapper = mapper.withColumnFieldPrefixes(columnFamily, columnFieldPrefixes);

        assertThat(accountTransactionMapper, notNullValue());
        assertThat(accountTransactionMapper.getColumnFieldPrefixes(columnFamily), notNullValue());
        assertThat(accountTransactionMapper.getColumnFieldPrefixes(columnFamily), hasSize(2));
        assertThat(accountTransactionMapper.getColumnFieldPrefixes(columnFamily).get(0), is(seqnum));
        assertThat(accountTransactionMapper.getColumnFieldPrefixes(columnFamily).get(1), is(txndate));
    }

    @Test
    public void testWithMultiColumnFieldPrefixMultiFamily() {
        AccountTransactionMapper mapper = new AccountTransactionMapper();
        List<String> columnFieldPrefixesCF1 = new ArrayList<String>();
        final String columnFamily = "cf";
        final String columnFamily2 = "cf2";
        final String seqnum = "seqnum";
        final String txndate = "txndate";

        columnFieldPrefixesCF1.add(seqnum);
        columnFieldPrefixesCF1.add(txndate);

        List<String> columnFieldPrefixesCF2 = new ArrayList<String>();
        final String fullMessage = "fullmessage";
        columnFieldPrefixesCF2.add(fullMessage);

        List<String> columnFamilies = new ArrayList<>();
        columnFamilies.add(columnFamily);
        columnFamilies.add(columnFamily2);

        mapper.withColumnFamilies(columnFamilies);

        AccountTransactionMapper accountTransactionMapper = mapper.withColumnFieldPrefixes(columnFamily, columnFieldPrefixesCF1);
        mapper.withColumnFieldPrefixes(columnFamily2, columnFieldPrefixesCF2);


        assertThat(accountTransactionMapper, notNullValue());
        assertThat(accountTransactionMapper.getColumnFieldPrefixes(columnFamily), notNullValue());
        assertThat(accountTransactionMapper.getColumnFieldPrefixes(columnFamily), hasSize(2));
        assertThat(accountTransactionMapper.getColumnFieldPrefixes(columnFamily).get(0), is(seqnum));
        assertThat(accountTransactionMapper.getColumnFieldPrefixes(columnFamily).get(1), is(txndate));
        // assert for CF2
        assertThat(accountTransactionMapper.getColumnFieldPrefixes(columnFamily2), notNullValue());
        assertThat(accountTransactionMapper.getColumnFieldPrefixes(columnFamily2), hasSize(1));
        assertThat(accountTransactionMapper.getColumnFieldPrefixes(columnFamily2).get(0), is(fullMessage));
    }

    @Test
    public void expectInvalidStateIfFieldPrefixForInvalidFamily() {
        AccountTransactionMapper mapper = new AccountTransactionMapper();
        List<String> columnFieldPrefixesCF1 = new ArrayList<String>();
        final String columnFamily = "cf";
        final String seqnum = "seqnum";

        columnFieldPrefixesCF1.add(seqnum);
        exception.expect(IllegalStateException.class);
        mapper.withColumnFieldPrefixes(columnFamily, columnFieldPrefixesCF1);

    }

    @Test
    public void testWithTransactionId() {
        AccountTransactionMapper mapper = new AccountTransactionMapper();
        final String transactionId = "SEQNUM";

        mapper = mapper.withTransactionId(transactionId);
        assertThat(mapper, notNullValue());
        assertThat(mapper.getTransactionId(), is(transactionId));
    }

    @Test
    public void testColumnsOneColumn() {
        AccountTransactionMapper mapper = new AccountTransactionMapper();
        List<String> columnFieldPrefixesCF1 = new ArrayList<String>();
        final String columnFamily = "cf";
        final String seqnumField = "seqnum";
        final String accNumField = "accnum";
        Long accNumValue = 123456l;

        columnFieldPrefixesCF1.add(accNumField);
        mapper.withRowKeyField("account-txnDate");
        mapper.withTransactionId(seqnumField);

        List<String> columnFamilies = new ArrayList<>();
        columnFamilies.add(columnFamily);

        mapper.withColumnFamilies(columnFamilies).withColumnFieldPrefixes(columnFamily, columnFieldPrefixesCF1);
        TridentTuple tuple = mock(TridentTuple.class);
        // set seqnum
        int seqnum = 123;
        when(tuple.getValueByField(seqnumField)).thenReturn(seqnum);
        // set account num
        when(tuple.getValueByField(accNumField)).thenReturn(accNumValue);

        String expectedAccNumCol = accNumField + "-" + seqnum;

        assertThat(mapper.columns(tuple), notNullValue());
        assertThat(mapper.columns(tuple).hasColumns(), is(true));
        assertThat(mapper.columns(tuple).getColumns(), hasSize(1));
        final ColumnList.Column column = mapper.columns(tuple).getColumns().get(0);

        assertThat(new String(column.getFamily()), is(columnFamily));
        assertThat(new String(column.getQualifier()), is(expectedAccNumCol));
        // check expected value
        assertThat(toLong(column.getValue()), is(accNumValue));
    }

    @Test
    public void testColumnsMultiColumns() {
        AccountTransactionMapper mapper = new AccountTransactionMapper();
        List<String> columnFieldPrefixesCF1 = new ArrayList<String>();
        final String columnFamily = "cf";
        final String seqnumField = "seqnum";
        final String accNumField = "accnum";
        final String txnDate = "txndate";
        Long accNumValue = 123456l;
        String txnDateValue = "2015-10-26";


        columnFieldPrefixesCF1.add(accNumField);
        columnFieldPrefixesCF1.add(txnDate);
        mapper.withRowKeyField("account-txnDate");
        mapper.withTransactionId(seqnumField);

        List<String> columnFamilies = new ArrayList<>();
        columnFamilies.add(columnFamily);

        mapper.withColumnFamilies(columnFamilies).withColumnFieldPrefixes(columnFamily, columnFieldPrefixesCF1);
        TridentTuple tuple = mock(TridentTuple.class);
        // set seqnum
        int seqnum = 123;
        when(tuple.getValueByField(seqnumField)).thenReturn(seqnum);
        when(tuple.getValueByField(accNumField)).thenReturn(accNumValue);
        when(tuple.getValueByField(txnDate)).thenReturn(txnDateValue);

        String expectedAccNumCol = accNumField + "-" + seqnum;
        String expectedTxnDateCol = txnDate + "-" + seqnum;

        assertThat(mapper.columns(tuple), notNullValue());
        assertThat(mapper.columns(tuple).hasColumns(), is(true));
        assertThat(mapper.columns(tuple).getColumns(), hasSize(2));
        final ColumnList.Column column = mapper.columns(tuple).getColumns().get(0);

        assertThat(new String(column.getFamily()), is(columnFamily));
        assertThat(new String(column.getQualifier()), is(expectedAccNumCol));
        // check expected value
        assertThat(toLong(column.getValue()), is(accNumValue));

        final ColumnList.Column column2 = mapper.columns(tuple).getColumns().get(1);

        assertThat(new String(column2.getFamily()), is(columnFamily));
        assertThat(new String(column2.getQualifier()), is(expectedTxnDateCol));
        // check expected value
        assertThat(new String(column2.getValue()), is(txnDateValue));
    }

    @Test
    public void testColumnsMultiFamiliesMultiColumns() {
        AccountTransactionMapper mapper = new AccountTransactionMapper();
        List<String> columnFieldPrefixesCF1 = new ArrayList<String>();
        List<String> columnFieldPrefixesCF2 = new ArrayList<String>();
        final String columnFamily = "cf";
        final String columnFamily2 = "cf2";
        final String seqnumField = "SEQNUM";
        final String accNumField = "accnum";
        final String txnDate = "txndate";
        final String fullmsg = "fullmsg";
        Long accNumValue = 123456l;
        String txnDateValue = "2015-10-26";
        String fullMessageValue = "{\"name\":\"message name\"}";

        columnFieldPrefixesCF1.add(accNumField);
        columnFieldPrefixesCF1.add(txnDate);
        columnFieldPrefixesCF2.add(fullmsg);
        mapper.withRowKeyField("account-txnDate");
        mapper.withTransactionId(seqnumField);

        List<String> columnFamilies = new ArrayList<>();
        columnFamilies.add(columnFamily);
        columnFamilies.add(columnFamily2);

        mapper.withColumnFamilies(columnFamilies).withColumnFieldPrefixes(columnFamily, columnFieldPrefixesCF1);
        mapper.withColumnFieldPrefixes(columnFamily2, columnFieldPrefixesCF2);

        TridentTuple tuple = mock(TridentTuple.class);
        // set seqnum
        int seqnum = 123;
        when(tuple.getValueByField(seqnumField)).thenReturn(seqnum);
        when(tuple.getValueByField(accNumField)).thenReturn(accNumValue);
        when(tuple.getValueByField(txnDate)).thenReturn(txnDateValue);
        when(tuple.getValueByField(fullmsg)).thenReturn(fullMessageValue);


        String expectedAccNumCol = accNumField + "-" + seqnum;
        String expectedTxnDateCol = txnDate + "-" + seqnum;
        String expectedFullMsg = fullmsg + "-" + seqnum;

        assertThat(mapper.columns(tuple), notNullValue());
        assertThat(mapper.columns(tuple).hasColumns(), is(true));
        assertThat(mapper.columns(tuple).getColumns(), hasSize(3));
        final ColumnList.Column column = mapper.columns(tuple).getColumns().get(0);

        assertThat(new String(column.getFamily()), is(columnFamily));
        assertThat(new String(column.getQualifier()), is(expectedAccNumCol));
        // check expected value
        assertThat(toLong(column.getValue()), is(accNumValue));

        final ColumnList.Column column2 = mapper.columns(tuple).getColumns().get(1);

        assertThat(new String(column2.getFamily()), is(columnFamily));
        assertThat(new String(column2.getQualifier()), is(expectedTxnDateCol));
        // check expected value
        assertThat(new String(column2.getValue()), is(txnDateValue));

        final ColumnList.Column column2_1 = mapper.columns(tuple).getColumns().get(2);

        assertThat(new String(column2_1.getFamily()), is(columnFamily2));
        assertThat(new String(column2_1.getQualifier()), is(expectedFullMsg));
        // check expected value
        assertThat(new String(column2_1.getValue()), is(fullMessageValue));
    }

    public static long toLong(byte[] b) {
        ByteBuffer bb = ByteBuffer.allocate(b.length);
        bb.put(b, 0, b.length);
        bb.flip();//need flip
        return bb.getLong();
    }

    public static byte[] toBytes(Long l) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.SIZE);
        buffer.putLong(l);
        return buffer.array();
    }
}