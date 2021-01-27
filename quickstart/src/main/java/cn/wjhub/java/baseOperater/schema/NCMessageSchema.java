package cn.wjhub.java.baseOperater.schema;

import cn.wjhub.java.baseOperater.entity.NCMessage;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * @author 张文军
 * @Description:
 * @Company:南京农业大学工学院
 * @version:1.0
 * @date 2021/1/232:06
 */
public class NCMessageSchema implements DeserializationSchema<NCMessage> {
    @Override
    public NCMessage deserialize(byte[] message) throws IOException {
        return new NCMessage(new String(message));
    }

    @Override
    public boolean isEndOfStream(NCMessage nextElement) {
        return false;
    }

    @Override
    public TypeInformation<NCMessage> getProducedType() {
        return TypeInformation.of(NCMessage.class);
    }
}
