package cn.wjhub.java.baseOperater.partitioner;

import cn.wjhub.java.baseOperater.entity.KafkaRecord;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;

/**
 * @author 张文军
 * @Description:
 * @Company:南京农业大学工学院
 * @version:1.0
 * @date 2021/1/2423:43
 */
public class KafkaSinkPartitioner  extends FlinkKafkaPartitioner<KafkaRecord> {

    /*创建固定大小分区 10 个*/

    @Override
    public int partition(KafkaRecord kafkaRecord, byte[] bytes, byte[] bytes1, String s, int[] ints) {
        return kafkaRecord.hashCode()%10;
    }
}
