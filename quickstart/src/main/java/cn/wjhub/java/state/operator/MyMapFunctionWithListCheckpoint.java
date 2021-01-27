package cn.wjhub.java.state.operator;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author 张文军
 * @Description:
 * @Company:南京农业大学工学院
 * @version:1.0
 * @date 2021/1/2518:00
 */
public class MyMapFunctionWithListCheckpoint extends RichMapFunction<String, String> implements ListCheckpointed<Integer> {
    private Integer count = 0;
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
    public void open(Configuration parameters) throws Exception {
        System.out.println("MyMapFuncitionWinthListCheckpoint.open");

    }

    @Override
    public List<Integer> snapshotState(long l, long l1) throws Exception {
        ArrayList list = new ArrayList<Integer>();
        if (list.isEmpty() || !count.equals(list.get(list.size() - 1))) {
            list.add(count);
        }
        System.out.println("MyMapFuncitionWinthListCheckpoint.snapshotState");
        System.out.println("Arrays.toString(list.toArray()) = " + Arrays.toString(list.toArray()));
        return list;
    }

    @Override
    public void restoreState(List<Integer> list) throws Exception {
        count = list.get(list.size() - 1);
    }
}
