package com.devcycle.explorestorm.function;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.devcycle.explorestorm.scheme.ExploreScheme;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * Takes message in ExploreScheme format and uses IP as key and concaternates the other fields into a pretty print string.
 *
 * Created by chrishowe-jones on 01/10/15.
 */
public class ExploreTransformMessage extends BaseFunction {

    public static String key = "msgkey";
    public static String value = "messageval";
    private static Fields emittedFields = new Fields(key, value);

    /**
     * Transforms the ExploreScheme message into a format for output to a Kafka output topic.
     *
     * @param tuple
     * @param collector
     */
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String keyValue = tuple.getStringByField(ExploreScheme.FIELD_IP_ADDRESS);
        String messageValue = ExploreScheme.prettyPrintTuple(tuple);
        collector.emit(new Values(keyValue, messageValue));
    }

    public static Fields getEmittedFields() {
        return emittedFields;
    }
}
