package com.devcycle.explorestorm.mapper;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.ITuple;
import backtype.storm.tuple.Values;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.hbase.bolt.mapper.HBaseValueMapper;

import java.util.ArrayList;
import java.util.List;

/**
 * Takes an HBase Explore Message result and returns a value list that has a value instance for each column and
 * corresponding value. So if result from HBase was
 * <pre>
 * eventTime, ipAddress, message
 * 2015-10-01, 123.456.789, some text
 * 2014-12-23, 456.789.000, some more text
 * </pre>
 *
 * this will return
 * <pre>
 * [eventTime, 2015-10-01]
 * [ipAddress, 123.456.789]
 * [message, some text]
 * [eventTime, 2014-12-23]
 * [ipAddress, 456.789.000]
 * [message, some more text]
 * </pre>
 *
 * Created by chrishowe-jones on 08/10/15.
 */
public class ExploreMessageValueMapper implements HBaseValueMapper {

    public List<Values> toValues(ITuple iTuple, Result result) throws Exception {
        List<Values> values = new ArrayList<Values>();
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            Values value = new Values(Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
            values.add(value);
        }
        return values;
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("columnName", "columnValue"));
    }
}
