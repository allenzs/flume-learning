package com.allen.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author allen
 * @create 2020-01-31 11:08
 */
public class TypeInterceptor implements Interceptor {

    // 声明一个存放事件的集合
    private List<Event> addHeaderEvents;

    @Override
    public void initialize() {
        addHeaderEvents = new ArrayList<Event>();
    }

    @Override  // TODO 单个时间拦截
    public Event intercept(Event event) {
        // 1 获取事件中的头信息
        Map<String, String> headers = event.getHeaders();

        // 2 获取事件中的body信息, 字节数组
        byte[] body = event.getBody();
        String strBody = new String(body);

        // 3 根据body中是否有“hello” 信息
        if (strBody.contains("hello")){
            headers.put("type","hello_type");
        }else{
            headers.put("type","no_hello_type");
        }

        return event;
        // 还可以做过滤，不包含 hello的数据，直接返回null
    }

    @Override  // TODO 批量事件拦截
    public List<Event> intercept(List<Event> list) {

        //1. 清空已经添加过头信息 的event 集合
        addHeaderEvents.clear();

        //2. 遍历events 给每一个事件添加头信息
        for (Event event : list) {
            // 3. 给每个event 添加头信息
            Event even = intercept(event);
            addHeaderEvents.add(even);
        }
        //4. 返回已经添加过头信息 的event 集合
        return addHeaderEvents;
    }

    @Override
    public void close() {

    }

    // 静态内部类，功能：构建方法
    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            // 构造方法私有，只能在静态内部类去调用构造方法。类似HBase中的一个类
            return new TypeInterceptor();
        }

        // 外面传入的配置文件，这里可以先读取到
        @Override
        public void configure(Context context) {

        }
    }
}
