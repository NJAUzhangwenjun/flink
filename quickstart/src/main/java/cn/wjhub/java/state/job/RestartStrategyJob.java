package cn.wjhub.java.state.job;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 张文军
 * @Description:
 * @Company:南京农业大学工学院
 * @version:1.0
 * @date 2021/1/250:35
 */
public class RestartStrategyJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());


        /**重启策略*/
        /*重启三次，每次立马重启*/
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(0)));
        final DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 9999);

        final SingleOutputStreamOperator<String> map = dataStreamSource.map(s -> {
            if (s.length() > 3) {
                int a = 1 / 0;
            }

            return s;
        }).setParallelism(1);
        map.print().setParallelism(1);


        env.execute("RestartStrategy");

    }
}
