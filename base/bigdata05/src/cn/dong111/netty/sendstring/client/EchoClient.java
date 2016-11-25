package cn.dong111.netty.sendstring.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.net.InetSocketAddress;

/**
 * @author chendong
 * @version [版本号, 2016-11-25]
 * @see [相关类/方法]
 * @since [产品/模块版本]
 *
 * 连接服务器+写数据到服务器+等待服务器返回相同的数据+关联连接
 */
public class EchoClient {

    private final String host;
    private final int port;

    public EchoClient(String host, int port) {
        this.host = host;
        this.port = port;
    }


    public static void main(String[] args) throws Exception {
        new EchoClient("localhost", 20000).start();
    }




    public void start() throws Exception {
        EventLoopGroup nioEventLoopGroup = null;
        try {
            // 客户端引导类
            Bootstrap bootstrap = new Bootstrap();
            // EventLoopGroup可以理解为是一个线程池，
            // 这个线程池用来处理连接、接受数据、发送数据
            nioEventLoopGroup = new NioEventLoopGroup();
            //多线程处理
            bootstrap.group(nioEventLoopGroup)
                    //指定通道类型为NioServerSocketChannel，一种异步模式，OIO阻塞模式为OioServerSocketChannel
                .channel(NioSocketChannel.class)
                    .remoteAddress(new InetSocketAddress(host,port))
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel socketChannel) throws Exception {
                            //注册handler
                            socketChannel.pipeline().addLast(new EchoClientHandler());
                        }
                    });
            //连接服务器
            ChannelFuture channelFuture = bootstrap.connect().sync();
            channelFuture.channel().closeFuture().sync();
        } finally {
           nioEventLoopGroup.shutdownGracefully().sync();
        }
    }
}
