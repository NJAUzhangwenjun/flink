package cn.wjhub.java.time.source;

import cn.wjhub.java.time.entity.NCTimeMessage;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.Arrays;
import java.util.Random;

import static java.lang.Math.abs;

/**
 * @author 张文军
 * @Description:
 * @Company:南京农业大学工学院
 * @version:1.0
 * @date 2021/1/273:44
 */
public class WithTimeStampWithWatermarkSource extends RichSourceFunction<NCTimeMessage> {
    private Boolean isCancel = false;
    private final String ms = "import org.apache.flink.streaming.api.functions.source.RichSourceFunction";
    private Random random;
    private char[] chars;

    @Override
    public void run(SourceContext<NCTimeMessage> sourceContext) throws Exception {


        while (!isCancel) {
            final NCTimeMessage message = sendMessage();
            synchronized (this) {
                sourceContext.collectWithTimestamp(message,message.getTimeStamp());
                sourceContext.emitWatermark(new Watermark(message.getTimeStamp()-3000));
            }
            try {
                Thread.sleep(abs(random.nextInt(1000) * 1L));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 产生数据
     * @return
     */
    private NCTimeMessage sendMessage() {
        final int a = random.nextInt(chars.length);
        final int b = random.nextInt(chars.length);
        String message = new String(Arrays.copyOfRange(this.chars, Math.min(a, b), Math.max(a, b)));
        final long l = System.currentTimeMillis();
        return new NCTimeMessage(message, l);

    }

    @Override
    public void cancel() {
        this.isCancel = true;

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.random = new Random();
        this.chars = ms.toCharArray();
    }
}
