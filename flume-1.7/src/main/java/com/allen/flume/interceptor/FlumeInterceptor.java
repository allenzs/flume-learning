package com.allen.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

public class FlumeInterceptor {
    private Logger logger = LoggerFactory.getLogger(FlumeInterceptor.class);
    private String appid;

    public FlumeInterceptor(String appid) {
        this.appid = appid;
    }
    public void initialize() {
    }

    public Event intercept(Event event) {
        StringBuilder builder = new StringBuilder();
        Map<String, String> headerMap = event.getHeaders();
        String ip;
        String projectName;
        if (null != headerMap && !headerMap.isEmpty()) {
            ip = headerMap.get("host");
            projectName = headerMap.get("projectname");
            builder.append("appid" + appid + ";ip:" + ip + ";projectname:" + projectName);
        }
        byte[] byteBody = event.getBody();
        String body = new String(byteBody, Charset.forName("utf-8"));
        builder.append(";body:" + body);
        event.setBody(builder.toString().trim().getBytes());
        logger.info("拼接后的body信息:" + builder.toString().trim());
        return event;
    }

    public List<Event> intercept(List<Event> events) {
        for (final Event event : events) {
            intercept(event);
        }
        return events;
    }

    public void close() {
    }

    public static class FlumeBuilder implements Interceptor.Builder {

        private String appId;
        @Override
        public Interceptor build() {
            return (Interceptor) new FlumeInterceptor(appId);
        }

        @Override
        public void configure(Context context) {
            if (appId==null){
                appId="999";
            }
            appId = context.getString("appId", "999");
        }
    }
}
