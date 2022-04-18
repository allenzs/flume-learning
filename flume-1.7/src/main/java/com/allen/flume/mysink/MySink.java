package com.allen.flume.mysink;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySink extends AbstractSink implements Configurable {

    // 定义两个属性：前后缀
    private String prefix;
    private String subfix;

    // 获取logger对象
    private Logger logger = LoggerFactory.getLogger(MySink.class);

    @Override
    public Status process() throws EventDeliveryException {
        //1 定义返回值
        Status status = null;
        //2 获取channel
        Channel channel = getChannel();
        //3 从Channel 获取事物
        Transaction transaction = channel.getTransaction();
        //4 开启事务
        transaction.begin();
        try {
            //5 从Channel 获取数据
            Event event = channel.take();

            if (event !=null) {
                //6 处理事件
                String strBody = new String(event.getBody());
                // 注意：这里的logger没有最大值限制。 自带的logger有最大值限制16个字节
                logger.info(prefix+strBody+subfix); // 打印
                logger.error(prefix+strBody+subfix); // 打印
            }
            //7 提交事务
            transaction.commit();
            //8 成功提交，修改状态
            status = Status.READY;

        } catch (ChannelException e) {
            e.printStackTrace();
            //9 提交事务失败
            transaction.rollback();
            //10 修改装填
            status = Status.BACKOFF;
        } finally {
            //11 关闭事务
            transaction.close();
        }
        //12 返回状态信息
        return status;
    }
    @Override
    public void configure(Context context) {
        // 读取配置文件，给前后缀赋值
        prefix = context.getString("prefix");
        subfix = context.getString("prefix", "default");
    }
}
