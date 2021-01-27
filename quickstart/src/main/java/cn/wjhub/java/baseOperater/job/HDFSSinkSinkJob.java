package cn.wjhub.java.baseOperater.job;

import cn.wjhub.java.baseOperater.entity.KafkaRecord;
import cn.wjhub.java.baseOperater.sink.HDFSSinkFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 张文军
 * @Description:
 * @Company:南京农业大学工学院
 * @version:1.0
 * @date 2021/1/2422:50
 */
public class HDFSSinkSinkJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        final DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

        final SingleOutputStreamOperator<KafkaRecord> map = source.map(s -> new KafkaRecord(s));
        map.addSink(new HDFSSinkFunction());


        env.execute("HDFSSinkJob");
    }
}
