/**
 * Put your copyright and license info here.
 */
package com.example.loadGenerator;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import org.apache.hadoop.conf.Configuration;

import java.util.List;
import java.util.Map;
import java.util.Properties;


@ApplicationAnnotation(name="KafkaLoadGenerator")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    EventGenerator eventGenerator = dag.addOperator("eventGenerator", new EventGenerator());
    KafkaOutput<String,String> kafkaOutput = dag.addOperator("kafkaOutput", new KafkaOutput()) ;

    Properties props = new Properties();
    props.setProperty("serializer.class", "kafka.serializer.StringEncoder");
    props.setProperty("partitioner.class", "com.example.loadGenerator.SimplePartitioner");
    props.put("metadata.broker.list", "node31:9092,node32:9092,node34:9092,node36:9092");
    props.setProperty("producer.type", "async");
    props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
//    props.setProperty("queue.buffering.max.messages", "20000");
//    props.setProperty("send.buffer.bytes", "" + 1048576);
//    props.setProperty("batch.num.messages", "5000");
    
    kafkaOutput.setConfigProperties(props);

    eventGenerator.init();
    Map<String, List<String>> campaigns = eventGenerator.getCampaigns();

    setupRedis(campaigns, "node35");

    dag.addStream("randomData", eventGenerator.out, kafkaOutput.inputPort).setLocality(DAG.Locality.THREAD_LOCAL);
    dag.setInputPortAttribute(kafkaOutput.inputPort, Context.PortContext.PARTITION_PARALLEL, true);
    dag.setAttribute(eventGenerator, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<EventGenerator>(20));
  }

  private void setupRedis(Map<String, List<String>> campaigns, String redis)
  {
    RedisHelper redisHelper = new RedisHelper();
    redisHelper.init(redis);

    redisHelper.prepareRedis(campaigns);
  }


}
