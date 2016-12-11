package cn.dong111.bigdata.hadooprpc.service;

import cn.dong111.bigdata.hadooprpc.protocol.ClientNamenodeProtocol;

/**
 * Created by chendong on 2016/12/11.
 */
public class MyNameNode implements ClientNamenodeProtocol{

    //模拟namenode业务方法之一:查询元数据
    @Override
    public String getMetaData(String path) {

        return path+": 3 - {BLK_1,BLK_2}……";
    }
}
