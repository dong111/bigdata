package cn.dong111.netty.sendobject.utils;

import java.io.*;

/**
 * Created by chendong on 2016/11/30.
 *
 * 字节流<-->对象装换工具类
 *
 */
public class ByteObjConverter {

    /**
     * 使用IO的InputStream流将byte[]装换为Object
     * @param bytes
     * @return
     */
    public static Object byteToObject(byte[] bytes){

        Object obj = null;
        ByteArrayInputStream bi = new ByteArrayInputStream(bytes);
        ObjectInputStream oi = null;

        try {
            oi = new ObjectInputStream(bi);
            obj = oi.readObject();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            //**一定要记得把流关闭
            try {
                bi.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                oi.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return obj;
    }

    /**
     * 使用IO的outputStream流将Object转换成byte[]
     * @param obj
     * @return
     */
    public static byte[] objectToByte(Object obj){
        byte[] bytes = null;
        ByteArrayOutputStream bo = new ByteArrayOutputStream();
        ObjectOutputStream oo = null;

        try {
            oo = new ObjectOutputStream(bo);
            oo.writeObject(obj);
            bytes = bo.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                bo.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            try {
                oo.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return bytes;
    }


}
