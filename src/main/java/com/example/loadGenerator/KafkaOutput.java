package com.example.loadGenerator;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.contrib.kafka.AbstractKafkaOutputOperator;
import kafka.producer.KeyedMessage;

/**
 * Created by sandesh on 3/4/16.
 */

public class KafkaOutput<K, V> extends AbstractKafkaOutputOperator<K, V> {

    public final transient DefaultInputPort<V> inputPort = new DefaultInputPort<V>() {
        @Override
        public void process(V v) {
            KafkaOutput.this.getProducer().send(new KeyedMessage(KafkaOutput.this.getTopic(),v, v));
            ++KafkaOutput.this.sendCount;
        }
    } ;

    public KafkaOutput() {

    }
}
