package com.example.loadGenerator;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

import java.util.Random;

/**
 * Created by sandesh on 3/3/16.
 */


public class SimplePartitioner implements Partitioner {

    Random random = new Random() ;
    public SimplePartitioner (VerifiableProperties props) {

    }

    public int partition(Object key, int a_numPartitions) {

        return random.nextInt(a_numPartitions);
    }

}
