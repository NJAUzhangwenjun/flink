package cn.wjhub.java.time.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author 张文军
 * @Description:
 * @Company:南京农业大学工学院
 * @version:1.0
 * @date 2021/1/273:45
 */
@Data
@AllArgsConstructor
public class NCTimeMessage {
    private String message;
    private Long timeStamp;
}
