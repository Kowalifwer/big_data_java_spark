package uk.ac.gla.dcs.bigdata.apps;

import org.apache.spark.util.AccumulatorV2;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MapAccumulator extends AccumulatorV2<Map<String, Integer>, Map<String, Integer>> {
    private ConcurrentHashMap<String, Integer> map;

    public MapAccumulator() {
        map = new ConcurrentHashMap<>();
    }

    @Override
    public boolean isZero() {
        return map.size() == 0;
    }

    @Override
    public AccumulatorV2<Map<String, Integer>, Map<String, Integer>> copy() {
        MapAccumulator newAccCopy = new MapAccumulator();
        newAccCopy.merge(this);
        return newAccCopy;
    }

    @Override
    public void reset() {
        map = new ConcurrentHashMap<>();
    }

    @Override
    public void add(Map<String, Integer> v) {
        v.forEach((key, value) -> {
            this.map.merge(key, 1, (oldValue, newValue) -> oldValue + newValue);
        });
    }

    @Override
    public void merge(AccumulatorV2<Map<String, Integer>, Map<String, Integer>> other) {
        other.value().forEach((key, value) -> {
            this.map.merge(key, value, (oldValue, newValue) -> oldValue + newValue);
        });
    }

    @Override
    public ConcurrentHashMap<String, Integer> value() {
        return this.map;
    }
}