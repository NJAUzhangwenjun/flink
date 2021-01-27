package cn.wjhub.java.state.operator;

import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

/**
 * @author 张文军
 * @Description:
 * @Company:南京农业大学工学院
 * @version:1.0
 * @date 2021/1/2519:17
 */
public class MyReduceFunctionWithKeyedState extends RichReduceFunction<Tuple2<String, Integer>> {
    private ValueState<Integer> count;

    @Override
    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
        /*故意设置一个Exception测试点*/
        if (t1.f0.length() > 3) {
            System.out.println("Error");
            int a = 1 / 0;
        }
        count.update(t1.f1 + t2.f1);
        System.out.println("count = " + count.value());
        return Tuple2.of(t1.f0, count.value());
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("MyReduceFunctionWithKeyedState.open");
        final ValueStateDescriptor<Integer> cs = new ValueStateDescriptor<>("cs", Integer.class);
        /** 超时时间设置*/
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.seconds(100))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();
        cs.enableTimeToLive(ttlConfig);

        count = getRuntimeContext().getState(cs);

    }
}
