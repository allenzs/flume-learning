package com.allen.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;
import java.util.Map;


/**
 * 限速拦截器
 */
public class SpeedInterceptor implements Interceptor {
    //速率:字节数/秒
    private int speed;

    //上次时间
    private long lastTime = -1;

    //上次body的大小
    private int lastBodySize = 0;

    private String zkConn;

    /**
     *
     */
    private SpeedInterceptor(int speed) {
        this.speed = speed;
    }

    /**
     * 初始化方法
     */
    public void initialize() {
    }

    /**
     *
     */
    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();
        int currentBodySize = event.getBody().length;
        System.out.println("currentBodySize : " + currentBodySize);
        long currentTime = System.currentTimeMillis();
        //第一次
        if (lastTime == -1) {
            this.lastTime = currentTime;
            this.lastBodySize = currentBodySize;

        }
        //第二次之后
        else {
            //1.计算按照指定速率，应该耗时多久
            long shouldMs = (long) ((double) lastBodySize / speed * 1000);
            //2.计算从上次发送的事件到目前经过了多长时间
            long elapse = currentTime - lastTime;

            if (elapse < shouldMs) {
                try {
                    //休眠
                    Thread.sleep(shouldMs - elapse);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            this.lastTime = System.currentTimeMillis();
            this.lastBodySize = currentBodySize;
        }
        return event;
    }

    /**
     *
     */
    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(event);
        }
        return events;
    }

    public void close() {
    }

    /**
     * Builder which builds new instances of the TimestampInterceptor.
     */
    public static class Builder implements Interceptor.Builder {

        private int speed;
        private final int SPEED_DEFAULT = 1024;


        public Interceptor build() {
            return new SpeedInterceptor(speed);
        }

        public void configure(Context context) {
            //提取配置文件中的速率设置
            String speedStr = context.getString(Constants.SPEED, "1024");
            this.speed = Integer.parseInt(speedStr);
            System.out.println("speed : " + speed);
        }
    }

    public static class Constants {
        public static String SPEED = "speed";
    }

}