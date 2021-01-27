package cn.wjhub.java.baseOperater.job;

import cn.wjhub.java.baseOperater.source.FileSourceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author 张文军
 * @Description:
 * @Company:南京农业大学工学院
 * @version:1.0
 * @date 2021/1/194:42
 */
public class FileSourceFunctionJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        final DataStreamSource<String> source = env.addSource(new FileSourceFunction());
        final SingleOutputStreamOperator<Tuple2<String, Integer>> flatMap = source.flatMap(new RichFlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                final String[] ss = value.replace("\\W", " ").split(" ");
                for (String s : ss) {
                    if (!s.isEmpty()) {
                        out.collect(new Tuple2<>(s, 1));
                    }
                }
            }
        });

        final SingleOutputStreamOperator<Tuple2<String, Integer>> sum = flatMap.keyBy(0).reduce(new RichReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
            }
        });
        sum.print().setParallelism(4);

        env.execute("FileSourceFunctionJob");
    }
}
