package cn.wjhub.java.state.job;

import cn.wjhub.java.state.StateUtils;
import cn.wjhub.java.state.operator.MyReduceFunctionWithKeyedState;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 张文军
 * @Description:
 * @Company:南京农业大学工学院
 * @version:1.0
 * @date 2021/1/2519:09
 */
public class KeyedStateJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        /**
         * 如果设置了checkpointing但是未设置重启策略，则会默认使用固定延迟重启策略，延迟时间是0，重启次数是Integer>MAX_VALUE
         * 所以，checkpoint得开发步骤是：
         *  1. 设置checkpointing
         *  2， 设置重启策略
         *  3. 设置state
         */
        /* 1. 设置checkpointing*/
        StateUtils.checkpointingSets(env);
        /*2， 设置重启策略*/
        /**重启策略*/
        /*重启三次，每次立马重启*/
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(0)));


        final DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 9999);

        final SingleOutputStreamOperator<Tuple2<String, Integer>> map = dataStreamSource.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return Tuple2.of(s, 1);
            }
        }).setParallelism(1);
        final SingleOutputStreamOperator<Tuple2<String, Integer>> reduce = map.keyBy(0).reduce(new MyReduceFunctionWithKeyedState()).setParallelism(1);
        reduce.print().setParallelism(1);
        env.execute("KeyedStateJob");
    }
}
