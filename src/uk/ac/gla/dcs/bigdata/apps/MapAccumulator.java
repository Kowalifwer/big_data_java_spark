package uk.ac.gla.dcs.bigdata.apps;

import org.apache.spark.util.AccumulatorV2;

import java.util.HashMap;
import java.util.Map;

public class MapAccumulator extends AccumulatorV2<Map<String, Integer>, Map<String, Integer>> {
    private Map<String, Integer> map = new HashMap<>();

    @Override
    public boolean isZero() {
        return map.isEmpty();
    }

    @Override
    public AccumulatorV2<Map<String, Integer>, Map<String, Integer>> copy() {
        MapAccumulator newAcc = new MapAccumulator();
        newAcc.map = new HashMap<>(map);
        return newAcc;
    }

    @Override
    public void reset() {
        map = new HashMap<>();
    }

    @Override
    public void add(Map<String, Integer> v) {
        for (Map.Entry<String, Integer> entry : v.entrySet()) {
            String key = entry.getKey();
            int value = entry.getValue();
            map.merge(key, value, Integer::sum);
        }
    }

    @Override
    public void merge(AccumulatorV2<Map<String, Integer>, Map<String, Integer>> other) {
        for (Map.Entry<String, Integer> entry : other.value().entrySet()) {
            String key = entry.getKey();
            int value = entry.getValue();
            map.merge(key, value, Integer::sum);
        }
    }

    @Override
    public Map<String, Integer> value() {
        return map;
    }
}