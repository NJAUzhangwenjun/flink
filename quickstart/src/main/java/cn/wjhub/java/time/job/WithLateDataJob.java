package cn.wjhub.java.time.job;

import cn.wjhub.java.time.entity.NCTimeMessage;
import cn.wjhub.java.time.source.WithTimeStampWithLateDateNoWatermarkSource;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author 张文军
 * @Description:
 * @Company:南京农业大学工学院
 * @version:1.0
 * @date 2021/1/273:29
 */
public class WithLateDataJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        /** 设置时间属性*/
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        final DataStreamSource<NCTimeMessage> source = env.addSource(new WithTimeStampWithLateDateNoWatermarkSource());
        final SingleOutputStreamOperator<NCTimeMessage> watermarkSource = source.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<NCTimeMessage>() {
            @Override
            public long extractAscendingTimestamp(NCTimeMessage element) {
                return element.getTimeStamp();
            }
        });
        final KeyedStream<NCTimeMessage, String> keyBy = watermarkSource.keyBy(new KeySelector<NCTimeMessage, String>() {
            @Override
            public String getKey(NCTimeMessage value) throws Exception {
                return value.getMessage();
            }
        });
        final WindowedStream<NCTimeMessage, String, TimeWindow> timeWindow = keyBy.timeWindow(Time.seconds(3));
        final OutputTag<NCTimeMessage> lateDataOutputTag = new OutputTag<NCTimeMessage>("ncLateData", TypeInformation.of(NCTimeMessage.class));

        final SingleOutputStreamOperator<NCTimeMessage> process = timeWindow
                .sideOutputLateData(lateDataOutputTag)
                .allowedLateness(Time.seconds(1))
                .process(new ProcessWindowFunction<NCTimeMessage, NCTimeMessage, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<NCTimeMessage> elements, Collector<NCTimeMessage> out) throws Exception {
                        System.out.println("_____________________________________________________");
                        System.out.println(
                                "subtask:" + getRuntimeContext().getIndexOfThisSubtask() +
                                        ",start:" + context.window().getStart() +
                                        ",end:" + context.window().getEnd() +
                                        ",waterMarks:" + context.currentWatermark() +
                                        ",currentTime:" + System.currentTimeMillis()
                        );
                        for (NCTimeMessage element : elements) {
//                            System.out.println("element = " + element);
                            out.collect(element);
                        }
                    }
                });

        final DataStream<NCTimeMessage> sideOutput = process.getSideOutput(lateDataOutputTag);
        sideOutput.print("sideOutput").setParallelism(1);
        process.print("timeWindow").setParallelism(1);

        env.execute("WithLateDataJob");
    }
}
