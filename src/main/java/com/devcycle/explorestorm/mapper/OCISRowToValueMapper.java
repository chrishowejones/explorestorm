package com.devcycle.explorestorm.mapper;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.ITuple;
import backtype.storm.tuple.Values;
import clojure.lang.MapEntry;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.hbase.bolt.mapper.HBaseValueMapper;
import parquet.org.slf4j.Logger;
import parquet.org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by chrishowe-jones on 29/10/15.
 */
public class OCISRowToValueMapper implements HBaseValueMapper {

    private static final Logger LOG = LoggerFactory.getLogger(OCISRowToValueMapper.class);

    private LinkedHashMap<String, Fields> fields = new LinkedHashMap<>();
    private List<String> columnFamilies = new ArrayList<>();

    public OCISRowToValueMapper(Map<String, Fields> cfFields) {
        if (cfFields == null)
            throw new IllegalArgumentException("Column Family Fields map cannot be null.");
        fields.putAll(cfFields);
        columnFamilies.addAll(fields.keySet());
    }

    @Override
    public List<Values> toValues(ITuple iTuple, Result result) throws Exception {
        List<Values> values = new ArrayList<Values>();
        for (String cf : columnFamilies) {
            byte[] columnFamilyBytes = Bytes.toBytes(cf);
            for (String fieldName : fields.get(cf).toList()) {
                Values value = new Values(Bytes.toString(result.getValue(columnFamilyBytes, Bytes.toBytes(fieldName))));
                values.add(value);
            }
        }
        return values;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(getFields());
    }

    public Fields getFields() {
        List<String> fieldNameList = new ArrayList<>();
        for (String cf : columnFamilies) {
            for (String fieldName : fields.get(cf).toList()) {
                fieldNameList.add(fieldName);
            }
        }
        Fields returnFields = new Fields(fieldNameList);
        return returnFields;
    }
}
