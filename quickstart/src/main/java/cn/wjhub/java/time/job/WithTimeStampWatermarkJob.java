package cn.wjhub.java.time.job;

import cn.wjhub.java.time.entity.NCTimeMessage;
import cn.wjhub.java.time.source.WithTimeStampWithWatermarkSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 张文军
 * @Description:
 * @Company:南京农业大学工学院
 * @version:1.0
 * @date 2021/1/273:29
 */
public class WithTimeStampWatermarkJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        /** 设置时间属性*/
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        final DataStreamSource<NCTimeMessage> source = env.addSource(new WithTimeStampWithWatermarkSource());

        source.print();

        env.execute("WithTimeStampWatermarkJob");
    }
}
