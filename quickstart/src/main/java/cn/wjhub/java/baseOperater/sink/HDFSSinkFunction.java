package cn.wjhub.java.baseOperater.sink;


import cn.wjhub.java.baseOperater.entity.KafkaRecord;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.text.SimpleDateFormat;

/**
 * @author 张文军
 * @Description:
 * @Company:南京农业大学工学院
 * @version:1.0
 * @date 2021/1/2421:26
 */
public class HDFSSinkFunction extends RichSinkFunction<KafkaRecord> {
    private FileSystem fileSystem;
    private String basePath;
    private SimpleDateFormat dateFormat;
    @Override
    public void open(Configuration parameters) throws Exception {
        final org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
        fileSystem = FileSystem.get(configuration);
        dateFormat = new SimpleDateFormat("yyyyMMddHH");
        basePath = new String("hdfs://hdfs/test/data");
    }

    @Override
    public void invoke(KafkaRecord value, Context context) throws Exception {
        final StringBuffer dataPath = new StringBuffer(basePath).append("\\").append(dateFormat).append("\\").append(getRuntimeContext().getTaskNameWithSubtasks());
        final Path path = new Path(dataPath.toString());
        if (!fileSystem.exists(path)) {
            fileSystem.create(path).write(value.toString().getBytes());
        } else {
            fileSystem.append(path).write(value.toString().getBytes());
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
