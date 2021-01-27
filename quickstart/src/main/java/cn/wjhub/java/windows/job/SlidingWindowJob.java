package cn.wjhub.java.windows.job;

import cn.wjhub.java.time.entity.NCTimeMessage;
import cn.wjhub.java.time.source.WithTimeStampWithWatermarkSource;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author 张文军
 * @Description:
 * @Company:南京农业大学工学院
 * @version:1.0
 * @date 2021/1/2722:15
 */
public class SlidingWindowJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        final DataStreamSource<NCTimeMessage> source = env.addSource(new WithTimeStampWithWatermarkSource());
        final KeyedStream<NCTimeMessage, String> keyedStream = source.keyBy(new KeySelector<NCTimeMessage, String>() {
            @Override
            public String getKey(NCTimeMessage value) throws Exception {
                return value.getMessage();
            }
        });
        final SingleOutputStreamOperator<Integer> process = keyedStream.window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(10)))
                .process(new ProcessWindowFunction<NCTimeMessage, Integer, String, TimeWindow>() {
            @Override
            public void process(String s, Context context, Iterable<NCTimeMessage> elements, Collector<Integer> out) throws Exception {
                Integer count = 0;
                for (NCTimeMessage element : elements) {
                    count++;
                }
                out.collect(count);
            }
        });
        process.print("count = ");

        env.execute("SlidingWindowJob");

    }
}
