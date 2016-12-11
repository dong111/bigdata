package cn.dong111.bigdata.hadooprpc.protocol;

/**
 * Created by chendong on 2016/12/11.
 */
public interface ClientNamenodeProtocol {

    public static final long versionID=1L; //会读取这个版本号， 但可以和客户端的不一样， 没有校验

    public String getMetaData(String path);

}
