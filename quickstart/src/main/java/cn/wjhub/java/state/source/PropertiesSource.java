package cn.wjhub.java.state.source;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author 张文军
 * @Description:
 * @Company:南京农业大学工学院
 * @version:1.0
 * @date 2021/1/2521:16
 */
public class PropertiesSource extends RichSourceFunction{
    private Properties properties;
    private MapState<String, String> mapState;
    private Boolean isCancel = false;
    @Override
    public void run(SourceContext sourceContext) throws Exception {
        while (!isCancel) {
            sourceContext.collect(mapState);
        }
    }

    /** 加载配置文件并转换为Map*/
    private Map loadProperties(Properties properties) throws IOException {
        final InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("testbroadcast.properties");
        properties.load(inputStream);
        Map<String, String> map = new HashMap<String, String>((Map) properties);
        return map;
    }

    @Override
    public void cancel() {
        this.isCancel = true;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.properties = new Properties();
        loadProperties(this.properties);
        final MapStateDescriptor<String, String> map = new MapStateDescriptor<>("map", String.class, String.class);
        /** 超时时间设置*/
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.seconds(100))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();
        map.enableTimeToLive(ttlConfig);
        mapState = getRuntimeContext().getMapState(map);
    }
}
