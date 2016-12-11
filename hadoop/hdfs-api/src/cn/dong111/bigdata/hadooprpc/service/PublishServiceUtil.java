package cn.dong111.bigdata.hadooprpc.service;

import cn.dong111.bigdata.hadooprpc.protocol.ClientNamenodeProtocol;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Builder;

import java.io.IOException;

/**
 * Created by chendong on 2016/12/11.
 */
public class PublishServiceUtil {

    public static void main(String[] args) throws IOException {
        Builder builder = new RPC.Builder(new Configuration());
        builder.setBindAddress("localhost")
                .setPort(8181)
                .setProtocol(ClientNamenodeProtocol.class)
                .setInstance(new MyNameNode());

        RPC.Server server = builder.build();
        server.start();
    }

}
