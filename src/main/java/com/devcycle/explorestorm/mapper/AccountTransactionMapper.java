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
     * Specify the columnFieldPrefixes for the given column family. These are the field names in the input tuple that will be used to
     * extract the values and to build the prefix of the column name in the HBase table.
     *
     * The full column name will be a concaternation of the field prefix and the transaction id value from the tuple.
     * E.g. for transaction 999 and field tIPTD, the column name will be tIPTD-999
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
            addColumnValues(tridentTuple, columns, family);
        }
        return columns;
    }

    /**
     * Specify the field name of the transaction id that will be used to create a composite column name
     * for the transaction fields.
     *
     * @param transactionId name of the tuple field containing the unique id of the transaction.
     * @return this
     */
    public AccountTransactionMapper withTransactionId(String transactionId) {
        this.transactionId = transactionId;
        return this;
    }

    /**
     * Return a list of the column field prefixes for the specified column family.
     *
     * @param columnFamily
     * @return list of matching column field prefixes.
     */
    public List<String> getColumnFieldPrefixes(String columnFamily) {
        return columnFieldPrefixes.get(columnFamily);
    }

    /**
     * Returns a list of the column family names.
     *
     * @return column families.
     */
    List<String> getColumnFamilies() {
        return columnFamilies;
    }

    /**
     * Returns the tuple field name of the unique identifier of the transaction.
     *
     * @return name of transaction id.
     */
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

    private void addColumnValues(TridentTuple tridentTuple, ColumnList columns, String family) {
        for (String fieldPrefix : columnFieldPrefixes.get(family)) {
            final Object valueByField = tridentTuple.getValueByField(fieldPrefix);
            if (valueByField != null) {
                columns.addColumn(family.getBytes(),
                        buildFieldName(fieldPrefix, tridentTuple.getValueByField(transactionId)),
                        toBytes(valueByField));
            }
        }
    }

    private byte[] buildFieldName(String fieldPrefix, Object transactionId) {
        String fieldName = fieldPrefix + "-" + ((Long) transactionId).toString();
        return fieldName.getBytes();
    }

}
