package cn.dong111.bigdata.hadooprpc.service;

import cn.dong111.bigdata.hadooprpc.protocol.ClientNamenodeProtocol;
import cn.dong111.bigdata.hadooprpc.protocol.IUserLoginService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Builder;

import java.io.IOException;

/**
 * Created by chendong on 2016/12/11.
 */
public class PublishUserLoginServiceUtil {

    public static void main(String[] args) throws IOException {
        Builder builder = new Builder(new Configuration());
        builder.setBindAddress("localhost")
                .setPort(8188)
                .setProtocol(IUserLoginService.class)
                .setInstance(new UserLoginServiceImpl());

        RPC.Server server = builder.build();
        server.start();
    }

}
