package cn.wjhub.java.baseOperater.job;

import cn.wjhub.java.baseOperater.entity.KafkaRecord;
import cn.wjhub.java.baseOperater.schema.KafkaRecordSchema;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;

import java.util.Properties;

/**
 * @author 张文军
 * @Description:
 * @Company:南京农业大学工学院
 * @version:1.0
 * @date 2021/1/202:25
 */
public class KafkaSourceJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        /** kafka */
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.17.5.116:9092");
        properties.setProperty("group.id", "flink_kafka_group");
        final FlinkKafkaConsumer09<KafkaRecord> kafkaConsumer = new FlinkKafkaConsumer09<>("flink_kafka", new KafkaRecordSchema(), properties);

        /** source */
        final DataStreamSource<KafkaRecord> source = env.addSource(kafkaConsumer);

        final DataStream<KafkaRecord> kafkaRecordDataStream = source.partitionCustom(new Partitioner<KafkaRecord>() {
            @Override
            public int partition(KafkaRecord key, int numPartitions) {
                return key.hashCode()%10;
            }
        }, new KeySelector<KafkaRecord, KafkaRecord>() {
            @Override
            public KafkaRecord getKey(KafkaRecord value) throws Exception {
                return value;
            }
        });

        source.print().setParallelism(4);

        env.execute("kafkaJob");
    }
}
