package cn.wjhub.java.baseOperater.job;

import cn.wjhub.java.baseOperater.entity.KafkaRecord;
import cn.wjhub.java.baseOperater.partitioner.KafkaSinkPartitioner;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;

import java.util.Properties;

/**
 * @author 张文军
 * @Description:
 * @Company:南京农业大学工学院
 * @version:1.0
 * @date 2021/1/2423:28
 */
public class KafkaSinkJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        final DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

        final SingleOutputStreamOperator<KafkaRecord> map = source.map(s -> new KafkaRecord(s));

        final Properties properties = new Properties();
        final FlinkKafkaProducer09<KafkaRecord> kafka_sink_topic = new FlinkKafkaProducer09<KafkaRecord>("kafka_Sink_topic", new SerializationSchema<KafkaRecord>() {
            @Override
            public byte[] serialize(KafkaRecord kafkaRecord) {
                return kafkaRecord.toString().getBytes();
            }
        },properties,new KafkaSinkPartitioner());

        map.addSink(kafka_sink_topic);

        env.execute("KafkaSinkJob");

    }
}
