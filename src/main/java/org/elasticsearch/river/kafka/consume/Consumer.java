package org.elasticsearch.river.kafka.consume;

/**
 * Created by rsl on 24-03-15.
 */
public interface Consumer {
    public void commitReads();
}
