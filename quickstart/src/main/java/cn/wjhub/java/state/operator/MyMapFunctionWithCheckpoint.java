package cn.wjhub.java.state.operator;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

/**
 * @author 张文军
 * @Description:
 * @Company:南京农业大学工学院
 * @version:1.0
 * @date 2021/1/2518:00
 */
public class MyMapFunctionWithCheckpoint extends RichMapFunction<String, String> implements CheckpointedFunction {
    private Integer count = 0;
    private ListState<Integer> ct;
    @Override
    public String map(String s) throws Exception {
        if (s.length() > 3) {
            System.out.println(" ERROR ");
            int a = 1 / 0;
        }
        System.out.println("count = " + ++count);
        return s;
    }


    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        System.out.println("MyMapFuncitionWinthCheckpoint.snapshotState");
        ct.clear();
        ct.add(count);
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        System.out.println("MyMapFuncitionWinthCheckpoint.initializeState");
        final ListStateDescriptor<Integer> ctd = new ListStateDescriptor<>("ctd", Integer.class);
        ct = functionInitializationContext.getOperatorStateStore().getListState(ctd);
        if (functionInitializationContext.isRestored()) {
            count = ct.get().iterator().next();
        }
    }
}
