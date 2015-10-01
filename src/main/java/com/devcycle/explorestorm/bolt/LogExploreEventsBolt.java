package com.devcycle.explorestorm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.devcycle.explorestorm.scheme.ExploreScheme;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by chrishowe-jones on 17/09/15.
 */
public class LogExploreEventsBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(LogExploreEventsBolt.class);

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        // none writes to logger
    }

    public void execute(Tuple tuple) {
        LOG.info("******MY MESSAGE ****** " + ExploreScheme.prettyPrintTuple(tuple));
        //System.out.println(ExploreScheme.prettyPrintTuple(tuple));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // no output
    }
}
