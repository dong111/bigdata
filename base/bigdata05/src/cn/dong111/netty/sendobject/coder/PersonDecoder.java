package cn.dong111.netty.sendobject.coder;

import cn.dong111.netty.sendobject.utils.ByteBufToBytes;
import cn.dong111.netty.sendobject.utils.ByteObjConverter;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

/**
 * Created by chendong on 2016/11/30.
 *
 * 继承netty对象解码器,实现字节流转换成对象
 */
public class PersonDecoder extends ByteToMessageDecoder {


    @Override
    protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf, List<Object> list) throws Exception {
        //工具类:将ByteBuf转换为byte[]
        ByteBufToBytes read = new ByteBufToBytes();
        byte[] bytes = read.read(byteBuf);
        //工具类:将byte[]装换成Object
        Object obj = ByteObjConverter.byteToObject(bytes);
        list.add(obj);
    }
}
