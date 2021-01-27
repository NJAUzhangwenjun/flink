package cn.wjhub.java.state.job;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
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
 * @date 2021/1/180:48
 */
public class StateFlink {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        final DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 9999);


        dataStreamSource.setBufferTimeout(1);

        /*final SingleOutputStreamOperator<String> flatmap = dataStreamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                final String[] ss = value.replaceAll("\\W", " ").split(" ");
                for (String s : ss) {
                    out.collect(s);
                }
            }
        });

        final SingleOutputStreamOperator<Tuple2<String, Integer>> map = flatmap.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return new Tuple2<>(value, 1);
            }
        });*/

        final SingleOutputStreamOperator<Tuple2<String, Integer>> map = dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                final String[] split = value.replaceAll("\\W", " ").split(" ");
                for (String s : split) {
                    out.collect(new Tuple2<>(s, 1));
                }
            }
        });
        final SingleOutputStreamOperator<Tuple2<String, Integer>> reduce = map.keyBy(0).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                return new Tuple2<>(value1.f0, value2.f1 + value1.f1);
            }
        });

        reduce.print();
        System.out.println(env.getExecutionPlan());

        env.execute("JavaJob");

    }
}
