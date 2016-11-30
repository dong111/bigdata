package cn.dong111.netty.sendobject.coder;

import cn.dong111.netty.sendobject.utils.ByteObjConverter;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * Created by chendong on 2016/11/30.
 *
 * 利用netty消息解码器将字节流序列化成对象
 */
public class PersonEncoder extends MessageToByteEncoder{

    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) throws Exception {
        //工具类:将objcet装换成byte[]
        byte[] datas = ByteObjConverter.objectToByte(msg);
        out.writeBytes(datas);
        ctx.flush();//记得写完后要刷新数据
    }
}
