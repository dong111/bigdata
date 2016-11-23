package cn.dong111.jms.mq.topic;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

/**
 * @author chendong
 * @version [版本号, 2016-11-23]
 * @see [相关类/方法]
 * @since [产品/模块版本]
 * JSM客户--JMS生产者
 *
 */
public class ProducerTool
{
    private String user = ActiveMQConnection.DEFAULT_USER;
    private String password = ActiveMQConnection.DEFAULT_PASSWORD;
    private String url = ActiveMQConnection.DEFAULT_BROKER_URL;
    private String subject = "dongTopic";
    private Connection connection = null;//JMS中间件连接
    private Session session = null;//生产者与JSM中间件的会话
    private MessageProducer producer = null;//消息生产者


    //初始化
    private void initialize() throws JMSException {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                user,password,url);
        Connection connection = connectionFactory.createConnection();
        //transacted 不开启本地事务  acknowledgeMode 使用默认值
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        //通过主题获取消息队列目标
        Destination destination = session.createTopic(subject);
        //通过目标创建消息生产者
        producer = session.createProducer(destination);
        //消息交付模式设置 设置成不可持续化模式，开销最小
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
    }
    //发送消息(生产消息)
    public void produceMessage(String message) throws JMSException {
        initialize();
        //创建发送的消息
        TextMessage msg = session.createTextMessage(message);
        System.out.println("Producer:->Sending message:"+message);
        producer.send(msg);
        System.out.println("Producer:->Message Send Complete!");
    }
    //关闭连接
    public void close() throws JMSException {
        System.out.println("Producer:->Closing connection");
        if(producer!=null)
            producer.close();
        if(session!=null)
            session.close();
        if(connection!=null)
            connection.close();
    }
}
