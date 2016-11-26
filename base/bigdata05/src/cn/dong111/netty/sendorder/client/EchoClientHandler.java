package cn.dong111.netty.sendorder.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * @author chendong
 * @version [版本号, 2016-11-25]
 * @see [相关类/方法]
 * @since [产品/模块版本]
 */
public class EchoClientHandler extends SimpleChannelInboundHandler<ByteBuf> {

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf) throws Exception {
        System.out.println("client 读取server数据..");
        // 服务端返回消息后
        byte[] req = new byte[byteBuf.readableBytes()];
        byteBuf.readBytes(req);
        String body = new String(req, "UTF-8");
        System.out.println("服务端数据为 :" + body);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        System.out.println("客户端连接服务器，开始发送数据……");
        byte[] req = "QUERY TIME ORDER".getBytes();//消息
        ByteBuf firstMessage = Unpooled.buffer(req.length);//发送类
        firstMessage.writeBytes(req);//发送
        ctx.writeAndFlush(firstMessage);//flush
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        System.out.println("clent exceptionCaugth……");
        ctx.close();
    }
}
