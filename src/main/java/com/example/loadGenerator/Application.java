/**
 * Put your copyright and license info here.
 */
package com.example.loadGenerator;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.contrib.kafka.KafkaSinglePortOutputOperator;
import org.apache.hadoop.conf.Configuration;

import java.util.Properties;

@ApplicationAnnotation(name="KafkaLoadGenerator")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    EventGenerator eventGenerator = dag.addOperator("eventGenerator", new EventGenerator());

    KafkaSinglePortOutputOperator<String,String> kafkaOutput = dag.addOperator("kafkaOutput", new KafkaSinglePortOutputOperator()) ;

    Properties props = new Properties();
    props.setProperty("serializer.class", "kafka.serializer.StringEncoder");
    props.put("metadata.broker.list", "node34:9092,node32:9092,node36:9092,node30:9092");
    props.setProperty("producer.type", "async");

    kafkaOutput.setTopic("benchmark_topic_8");
    kafkaOutput.setConfigProperties(props);

    dag.addStream("randomData", eventGenerator.out, kafkaOutput.inputPort);
    dag.setInputPortAttribute(kafkaOutput.inputPort, Context.PortContext.PARTITION_PARALLEL, true);
    dag.setAttribute(eventGenerator, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<EventGenerator>(4));
  }
}
