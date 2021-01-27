package cn.wjhub.java.time.job;

import cn.wjhub.java.time.entity.NCTimeMessage;
import cn.wjhub.java.time.source.WithTimeStampNoWatermarkSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author 张文军
 * @Description:
 * @Company:南京农业大学工学院
 * @version:1.0
 * @date 2021/1/273:29
 */
public class OperatorWatermarkJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        /** 设置时间属性*/
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.getConfig().setAutoWatermarkInterval()
        final DataStreamSource<NCTimeMessage> source = env.addSource(new WithTimeStampNoWatermarkSource());


        /**
         * 指定时间戳和Watermark
         * */
        /*1. 周期时间watermark*/
        /*final SingleOutputStreamOperator<NCTimeMessage> watermarksSource = source.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<NCTimeMessage>() {
            private Long watermark;

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(watermark);
            }

            @Override
            public long extractTimestamp(NCTimeMessage element, long previousElementTimestamp) {
                watermark = element.getTimeStamp() - 1000L;
                return element.getTimeStamp();
            }
        });

        watermarksSource.print();*/

       /* *//* 2. 根据事件触发得 watermark*//*
        final SingleOutputStreamOperator<NCTimeMessage> watermarksSource = source.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<NCTimeMessage>() {
            @Nullable
            @Override
            public Watermark checkAndGetNextWatermark(NCTimeMessage lastElement, long extractedTimestamp) {
                if (lastElement.getMessage().contains("flink")) {
                    return new Watermark(lastElement.getTimeStamp() - 1000L);
                }
                return null;
            }

            @Override
            public long extractTimestamp(NCTimeMessage element, long previousElementTimestamp) {
                return element.getTimeStamp();
            }
        });*/
        /*3. 系统自带的：数据是升序*/
        /*final SingleOutputStreamOperator<NCTimeMessage> watermarksSource = source.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<NCTimeMessage>() {
            @Override
            public long extractAscendingTimestamp(NCTimeMessage element) {
                return element.getTimeStamp();
            }
        });*/
        /*4. 系统自带：处理延迟数据*/
        final SingleOutputStreamOperator<NCTimeMessage> watermarksSource = source.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<NCTimeMessage>(Time.seconds(10)) {
            @Override
            public long extractTimestamp(NCTimeMessage element) {
                return element.getTimeStamp();
            }
        });

        watermarksSource.print();
        env.execute("OperatorWatermarkJob");
    }
}
