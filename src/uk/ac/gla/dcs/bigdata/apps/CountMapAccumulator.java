package uk.ac.gla.dcs.bigdata.apps;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.spark.util.AccumulatorV2;

/**
 * This is a custom Spark Accumulator, which can be used to accumulate string:integer maps. The accumulator will increase the integer count values of existing keys, rather than overwriting them.
 * This accumulator is thread-safe, and is used to merge the token counts from each partition, into a single countMap.
 * 
 * @author Artem, Roman
 *
 */
public class CountMapAccumulator extends AccumulatorV2<Map<String, Integer>, Map<String, Integer>> {
    //this is the countMap that will be merged with other countMaps. It is a ConcurrentHashMap, which is thread-safe.
    private ConcurrentHashMap<String,Integer> countMap = new ConcurrentHashMap<>();
    
    /**
     * Returns true if this accumulator is empty.
     * @return true if this accumulator is empty.
     */
    @Override
    public boolean isZero() {
        return countMap.isEmpty();
    }

    /**
     * Creates and returns a new copy of this accumulator object.
     * @return a new copy of this accumulator object
     */
    @Override
    public AccumulatorV2<Map<String, Integer>, Map<String, Integer>> copy() {
        CountMapAccumulator accumulatorCopy = new CountMapAccumulator();
        accumulatorCopy.countMap.putAll(this.countMap);
        return accumulatorCopy;
    }

    /**
     * Resets this accumulator to an empty countMap.
     */
    @Override
    public void reset() {
        countMap.clear();
    }

    /**
     * Adds a map of key-value pairs to this accumulator.
     * If a key already exists in this accumulator, its value is increased by the value from the input countMap.
     * @param inputCountMap the map of key-value pairs to be added to this accumulator
     */
    @Override
    public void add(Map<String, Integer> inputCountMap) {
        for (Map.Entry<String,Integer> entry : inputCountMap.entrySet()) {
            String key = entry.getKey();
            int value = entry.getValue();
            //this is the merge function. If the key already exists, then the value is added to the existing value. If the key does not exist, then the key is added to the countMap with the value.
            countMap.merge(key,value,(v1,v2) -> v1 + v2);
        }
    }

    /**
     * Merges another map accumulator with this one by adding their values for the same keys.
     * @param other the other map accumulator to be merged
     */
    @Override
    public void merge(AccumulatorV2<Map<String, Integer>, Map<String, Integer>> otherCountMapAccumulator) {
        this.add(otherCountMapAccumulator.value());
    }

    /**
     * Returns the current state of this accumulators countMap.
     * @return the current state of this accumulators countMap
     */
    @Override
    public ConcurrentHashMap<String, Integer> value() {
        return this.countMap;
    }
}