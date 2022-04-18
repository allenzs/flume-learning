package com.allen.flume.interceptor;

import com.allen.flume.util.RedisUtil;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;


/**
 * 容灾拦截器
 * flume重启后，避免日志的重复收集。
 */
public class FailoverInterceptor implements Interceptor {
	private FailoverInterceptor() {
	}

	/**
	 *初始化方法
	 */
	public void initialize() {
	}

	/**
	 */
	public Event intercept(Event event) {
		String line = new String(event.getBody());
		String key = line.substring(0,line.indexOf("{"));
		if(RedisUtil.existsInRedis(key)){
			return null ;
		}
		else{
			RedisUtil.saveToRedis(key);
			return event;
		}
	}

	/**
	 */
	public List<Event> intercept(List<Event> events) {
		List<Event> copy = new ArrayList<Event>() ;
		for(Event e: events){
			String line = new String(e.getBody());
			String key = line.substring(0, line.indexOf("{"));
			if (!RedisUtil.existsInRedis(key)) {
				RedisUtil.saveToRedis(key);
				copy.add(e) ;
			}
		}
		return copy;
	}

	public void close() {
	}

	/**
	 * Builder which builds new instances of the TimestampInterceptor.
	 */
	public static class Builder implements Interceptor.Builder {
		public Interceptor build() {
			return new FailoverInterceptor();
		}

		public void configure(Context context) {
		}
	}
}