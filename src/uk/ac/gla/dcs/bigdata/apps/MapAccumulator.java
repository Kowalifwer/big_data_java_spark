package uk.ac.gla.dcs.bigdata.apps;

import org.apache.spark.util.AccumulatorV2;

import java.util.HashMap;

public class MapAccumulator extends AccumulatorV2<HashMap<String, Integer>, HashMap<String, Integer>> {
    private HashMap<String, Integer> map;

    public MapAccumulator() {
        map = new HashMap<>();
    }

    @Override
    public boolean isZero() {
        return map.size() == 0;
    }

    @Override
    public AccumulatorV2<HashMap<String, Integer>, HashMap<String, Integer>> copy() {
        MapAccumulator newAccCopy = new MapAccumulator();
        newAccCopy.merge(this);
        return newAccCopy;
    }

    @Override
    public void reset() {
        map = new HashMap<>();
    }

    @Override
    public void add(HashMap<String, Integer> v) {
        v.forEach((key, value) -> {
            this.map.merge(key, 1, (oldValue, newValue) -> oldValue + newValue);
        });
    }

    @Override
    public void merge(AccumulatorV2<HashMap<String, Integer>, HashMap<String, Integer>> other) {
        other.value().forEach((key, value) -> {
            this.map.merge(key, value, (oldValue, newValue) -> oldValue + newValue);
        });
    }

    @Override
    public HashMap<String, Integer> value() {
        return this.map;
    }
}