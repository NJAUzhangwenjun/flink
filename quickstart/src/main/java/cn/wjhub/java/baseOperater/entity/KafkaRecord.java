package cn.wjhub.java.baseOperater.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @author 张文军
 * @Description:
 * @Company:南京农业大学工学院
 * @version:1.0
 * @date 2021/1/202:25
 */
@Data
@AllArgsConstructor
@EqualsAndHashCode
public class KafkaRecord {
    private String record;
}
