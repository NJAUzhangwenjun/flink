package cn.wjhub.java.baseOperater.schema;

import cn.wjhub.java.baseOperater.entity.KafkaRecord;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * @author 张文军
 * @Description: 序列化和反序列化实现
 * @Company:南京农业大学工学院
 * @version:1.0
 * @date 2021/1/202:30
 */
public class KafkaRecordSchema implements DeserializationSchema<KafkaRecord> {
    @Override
    public KafkaRecord deserialize(byte[] message) throws IOException {
        return new KafkaRecord(new String(message));
    }

    @Override
    public boolean isEndOfStream(KafkaRecord nextElement) {
        return false;
    }

    @Override
    public TypeInformation<KafkaRecord> getProducedType() {
        return TypeInformation.of(KafkaRecord.class);
    }
}
