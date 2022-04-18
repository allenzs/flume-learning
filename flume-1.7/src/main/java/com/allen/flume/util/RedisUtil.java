package com.allen.flume.util;

import redis.clients.jedis.Jedis;

/**
 * redis工具类
 */
public class RedisUtil {
	public static Jedis redis = new Jedis("s101", 6379);
	/**
	 * 写入指定key到redis中。
	 */
	public static void saveToRedis(String line) {
		redis.set(line , "1") ;
		//3天过期
		redis.expire(line , 3 * 24 * 60 * 60) ;
 	}

	/**
	 * 判断line数据在redis中是否存在
	 */
	public static boolean existsInRedis(String line) {
		return redis.exists(line);
	}
}
