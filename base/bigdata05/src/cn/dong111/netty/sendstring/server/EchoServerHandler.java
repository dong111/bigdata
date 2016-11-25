package cn.dong111.netty.sendstring.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.Date;

/**
 * @author chendong
 * @version [版本号, 2016-11-25]
 * @see [相关类/方法]
 * @since [产品/模块版本]
 */
public class EchoServerHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        System.out.println("server 读取数据……");
        //读取数据
        ByteBuf buf = (ByteBuf) msg;
        byte[] req =new byte[buf.readableBytes()];//构造刻度长度内容字节数字
        buf.readBytes(req);
        String body = new String(req,"UTF-8");
        System.out.println("接收客户端数据:"+body);
        //向客户端写数据
        System.out.println("server向client发送数据");
        String currentTime = new Date(System.currentTimeMillis()).toString();
        ByteBuf resp = Unpooled.copiedBuffer(currentTime.getBytes());
        ctx.write(resp);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        System.out.println("server 读取数据完毕……");
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }
}
