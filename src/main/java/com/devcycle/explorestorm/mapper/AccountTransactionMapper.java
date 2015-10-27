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
     * @param columnFamily name of column family
     * @return this
     */
    public AccountTransactionMapper withColumnFamilies(String columnFamily) {
        columnFamilies.add(columnFamily);
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
                        buildFieldName(fieldPrefix, tridentTuple.getValueByField(rowKeyField)),
                        toBytes(tridentTuple.getValueByField(fieldPrefix)));
            }
        }
        return columns;
    }

    private byte[] buildFieldName(String fieldPrefix, Object rowKeyValue) {
        String fieldName = fieldPrefix + ":" + ((Integer)rowKeyValue).toString();
        return fieldName.getBytes();
    }

    public List<String> getColumnFieldPrefixes(String columnFamily) {
        return columnFieldPrefixes.get(columnFamily);
    }


    List<String> getColumnFamilies() {
        return columnFamilies;
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
