package cn.wjhub.java.baseOperater.transformations;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.CopyOnWriteArrayList;


/**
 * @author 张文军
 * @Description:
 * @Company:南京农业大学工学院
 * @version:1.0
 * @date 2021/1/231:43
 */
public class UnionJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        final DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 9000).setParallelism(1);

        final DataStreamSource<String> ncMessage = env.socketTextStream("localhost", 9999).setParallelism(1);

        final DataStream<String> union = dataStreamSource.union(ncMessage);
        final SingleOutputStreamOperator<Tuple1<String>> map = ncMessage.map(s -> {
            return new Tuple1<String>(s);
        }).returns(Types.TUPLE());

        final ConnectedStreams<String, Tuple1<String>> connect = union.connect(map);
        final SingleOutputStreamOperator<Tuple2<String, String>> process = connect.process(new CoProcessFunction<String, Tuple1<String>, Tuple2<String, String>>() {
            CopyOnWriteArrayList<String> list = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                list = new CopyOnWriteArrayList<String>();
            }

            @Override
            public void processElement1(String value, Context ctx, Collector<Tuple2<String, String>> out) throws Exception {
                if (!value.isEmpty()) {
                    list.add(value);
                    out.collect(new Tuple2<>(value, "empty"));
                } else {
                    out.collect(null);
                }
            }

            @Override
            public void processElement2(Tuple1<String> value, Context ctx, Collector<Tuple2<String, String>> out) throws Exception {
                if (!list.isEmpty()) {
                    out.collect(new Tuple2<>(list.remove(list.size() - 1), value.f0));
                } else {
                    out.collect(new Tuple2<>("empty", value.f0));
                }
            }
        }).returns(Types.TUPLE());
        process.print().setParallelism(2);


        env.execute("UnionJob");
    }
}
