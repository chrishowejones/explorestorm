package com.devcycle.explorestorm.mapper;

import org.apache.storm.hbase.common.ColumnList;
import org.apache.storm.hbase.trident.mapper.TridentHBaseMapper;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.storm.hbase.common.Utils.toBytes;

/**
 * Created by chrishowe-jones on 26/10/15.
 */
public class AccountTransactionMapper implements TridentHBaseMapper {

    private String rowKeyField;
    private List<String> columnFamilies = new ArrayList<String>();
    private Map<String, List<String>> columnFieldPrefixes = new HashMap<String, List<String>>();
    private String transactionId;

    /**
     * Specify the RowKey field name.
     *
     * @param rowKeyField row key field name.
     * @return this
     */
    public AccountTransactionMapper withRowKeyField(String rowKeyField) {
        this.rowKeyField = rowKeyField;
        return this;
    }

    /**
     * Specify the ColumnFamily names
     *
     * @param columnFamilies list of names of column family
     * @return this
     */
    public AccountTransactionMapper withColumnFamilies(List<String> columnFamilies) {
        this.columnFamilies = columnFamilies;
        return this;
    }

    /**
     * Specify the columnFieldPrefixes for the given column family.
     *
     * @param columnFieldPrefixes prefixes for column field names.
     * @return this
     */
    public AccountTransactionMapper withColumnFieldPrefixes(String columnFamily, List<String> columnFieldPrefixes) {
        if (!columnFamilies.contains(columnFamily))
            throw new IllegalStateException("Column family " + columnFamily + " is not a known column family.");
        this.columnFieldPrefixes.put(columnFamily, columnFieldPrefixes);
        return this;
    }


    /**
     * Returns the row key for the given trident tuple.
     *
     * @param tridentTuple
     * @return row key bytes consisting of the column name string (in bytes) and the value from the tuple.
     */
    @Override
    public byte[] rowKey(TridentTuple tridentTuple) {
        final Object rowKeyValue = tridentTuple.getValueByField(rowKeyField);
        byte[] rowKeyBytes = null;
        if (rowKeyValue != null)
            rowKeyBytes = toBytes(rowKeyValue);
        return rowKeyBytes;
    }

    /**
     * Returns the columns for this tuple. The column name will consist of dynamically generated
     * concaternation of the static field name and the sequence number from the data in the tuple, separated
     * by a ':'.
     *
     * @param tridentTuple
     * @return list of columns with column names consisting of the static field name and sequence number,
     * the values will be the values from the tuple.
     */
    @Override
    public ColumnList columns(TridentTuple tridentTuple) {
        ColumnList columns = new ColumnList();
        for (String family : columnFamilies) {
            for (String fieldPrefix : columnFieldPrefixes.get(family)) {
                columns.addColumn(family.getBytes(),
                        buildFieldName(fieldPrefix, tridentTuple.getValueByField(transactionId)),
                        toBytes(tridentTuple.getValueByField(fieldPrefix)));
            }
        }
        return columns;
    }

    public AccountTransactionMapper withTransactionId(String transactionId) {
        this.transactionId = transactionId;
        return this;
    }



    private byte[] buildFieldName(String fieldPrefix, Object transactionId) {
        String fieldName = fieldPrefix + "-" + ((Integer)transactionId).toString();
        return fieldName.getBytes();
    }

    public List<String> getColumnFieldPrefixes(String columnFamily) {
        return columnFieldPrefixes.get(columnFamily);
    }


    List<String> getColumnFamilies() {
        return columnFamilies;
    }

    String getTransactionId() {
        return transactionId;
    }

    /**
     * Return row key field.
     *
     * @return row key field.
     */
    String getRowKeyField() {
        return rowKeyField;
    }

}
