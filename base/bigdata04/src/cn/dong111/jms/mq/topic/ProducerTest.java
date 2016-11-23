package cn.dong111.jms.mq.topic;

import javax.jms.JMSException;
import java.util.Random;

/**
 * @author chendong
 * @version [版本号, 2016-11-23]
 * @see [相关类/方法]
 * @since [产品/模块版本]
 *
 * 消息生产者向JMS中间件发送消息
 */
public class ProducerTest
{
    public static void main(String[] args) throws InterruptedException, JMSException {
        ProducerTool producerTool = new ProducerTool();
        Random random = new Random();
        for (int i = 0; i < 20; i++) {
            Thread.sleep(random.nextInt(10)*100);
            producerTool.produceMessage("Hello,world!--"+i);
            producerTool.close();
        }
    }
}
