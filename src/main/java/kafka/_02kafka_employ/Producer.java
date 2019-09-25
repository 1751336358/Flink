package kafka._02kafka_employ;


import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import pojo.Employ;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * Hello world!
 *  ./kafka-producer-perf-test.sh  --topic test_kafka_perf1 --num-records 100000000 --record-size 687  --producer-props   bootstrap.servers=10.240.1.134:9092,10.240.1.143:9092,10.240.1.146:9092  batch.size=10000   --throughput 30000
 *  造数据脚本
 */
public class Producer {
    public static void main( String[] args ) throws Exception{

        Properties props = new Properties();
        props.put("bootstrap.servers","192.168.234.130:9092");
        props.put("acks","-1");
        props.put("retries",3);
        props.put("batch.size",16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        org.apache.kafka.clients.producer.Producer<String, String> producer = new KafkaProducer<String, String>(props);
        String topicName = "employ_topic";
        Long id = 1L;
        for(int i = 1; i <= 100000; i++){
            Employ employ = new Employ(id++,"employ"+i,i);
            Future<RecordMetadata> send = producer.send(new ProducerRecord<String, String>(topicName,
                    Integer.toString(i), JSON.toJSONString(employ)));
            System.out.println("offset="+send.get().offset()+",partition="+send.get().partition()+",topic="+send.get().topic());
            Thread.sleep(100);
        }
        producer.close();
        producer.close();
    }
}
