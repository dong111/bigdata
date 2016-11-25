package cn.dong111.netty.sendstring.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * @author chendong
 * @version [版本号, 2016-11-25]
 * @see [相关类/方法]
 * @since [产品/模块版本]
 *
 * 配置服务器功能，如线程，端口，实现服务器处理程序，它包含业务逻辑
 * 决定当有个一个请求连接或接受数据时候做什么
 *
 */
public class EchoServer {

    private  int port;

    public EchoServer(int port) {
        this.port = port;
    }


    public static void main(String[] args) {
        new  EchoServer(20000).start();
    }

    private void start() {
        EventLoopGroup eventLoopGroup = null;
        //服务端引导类
        ServerBootstrap serverBootstrap = new ServerBootstrap();

        //连接池处理数据
        eventLoopGroup = new NioEventLoopGroup();
        //配置bootstrap
        serverBootstrap.group(eventLoopGroup)
                //指定通道类型为NioServerSocketChannel
                //一中异步模式，OIO阻塞模式为OloServerSocketChannel
                .channel(NioServerSocketChannel.class)
                .localAddress("localhost",port)//设置服务端端口
                .childHandler(new ChannelInitializer<Channel>() {
                    //设置childHandler执行所有的连接请求
                    @Override
                    protected void initChannel(Channel channel) throws Exception {
                        channel.pipeline().addLast(new EchoServerHandler());//注册handdler
                    }
                });

        try {
            //最后绑定服务器等待直到绑定完成，调用sync()方法会阻塞直到服务器完成绑定，
            //然后服务器等待通道关闭，因为使用sync(),所有关闭操作也会被阻塞
            ChannelFuture channelFuture = serverBootstrap.bind().sync();
            System.out.println("开始监听，端口为："+channelFuture.channel().localAddress());
            channelFuture.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }






}
