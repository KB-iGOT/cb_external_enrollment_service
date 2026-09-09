package com.igot.cb.transactional.cassandrautils;

import java.util.Map;

/**
 * One row's worth of counter deltas for a {@link CassandraOperation#incrementCounters} call:
 * the composite key identifying the counter row, and the column deltas to apply to it.
 */
public class CounterIncrement {

    private final Map<String, Object> compositeKey;
    private final Map<String, Long> counterDeltas;

    public CounterIncrement(Map<String, Object> compositeKey, Map<String, Long> counterDeltas) {
        this.compositeKey = compositeKey;
        this.counterDeltas = counterDeltas;
    }

    public Map<String, Object> getCompositeKey() {
        return compositeKey;
    }

    public Map<String, Long> getCounterDeltas() {
        return counterDeltas;
    }
}
