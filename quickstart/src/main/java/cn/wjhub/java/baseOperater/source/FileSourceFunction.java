package cn.wjhub.java.baseOperater.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.*;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author 张文军
 * @Description:
 * @Company:南京农业大学工学院
 * @version:1.0
 * @date 2021/1/193:39
 */
public class FileSourceFunction implements SourceFunction<String> {
    private Boolean isCancel = false;
    private static String path = "E:/e/workeSpace/bigdata/flink/";
    private static CopyOnWriteArrayList<WatchEvent<?>> eventList = new CopyOnWriteArrayList<>();

    public FileSourceFunction() {
        super();
        new Thread(() -> {
            try {
                watchFiles(path);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();
    }

    @Override
    public void run(SourceContext<String> ctx) {
        while (!isCancel) {
            InputStreamReader reader = null;
            BufferedReader bufferedReader = null;
            try {
                if (!eventList.isEmpty()) {
                    final WatchEvent<?> event = eventList.remove(eventList.size() - 1);
                    reader = new InputStreamReader(new FileInputStream(path + event.context().toString().substring(0, event.context().toString().length() - 1)));
                    bufferedReader = new BufferedReader(reader);
                    String line = bufferedReader.readLine();
                    while (line != null) {
                        ctx.collect(line);
                        line = bufferedReader.readLine();
                    }


                }
                Thread.sleep(100);
            } catch (Exception e) {
            }  finally {
                try {
                    if (bufferedReader != null) {
                        bufferedReader.close();
                    }
                    if (reader != null) {
                        reader.close();
                    }
                } catch (Exception e) {

                }
            }
        }
    }

    @Override
    public void cancel() {
        this.isCancel = true;
    }

    /**
     * 监听文件变化
     *
     * @param path 路径
     * @throws IOException
     * @throws InterruptedException
     */
    private void watchFiles(String path) throws IOException, InterruptedException {
        WatchService watchService = FileSystems.getDefault().newWatchService();

        Paths.get(path).register(watchService,
                StandardWatchEventKinds.ENTRY_CREATE,
                StandardWatchEventKinds.ENTRY_DELETE,
                StandardWatchEventKinds.ENTRY_MODIFY
        );
        while (true) {
            WatchKey key = watchService.take();
            final List<WatchEvent<?>> eventList = key.pollEvents();

            if (eventList.size() > 0) {
                FileSourceFunction.eventList.addAll(eventList);
            }
            for (WatchEvent<?> event : eventList) {
                System.out.println(event.context() + "：发生了：" + event.kind() + "事件；");
            }
            if (!key.reset()) {
                break;
            }
        }
    }

}
