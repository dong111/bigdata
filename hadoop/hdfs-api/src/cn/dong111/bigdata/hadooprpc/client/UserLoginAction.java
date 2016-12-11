package cn.dong111.bigdata.hadooprpc.client;

import cn.dong111.bigdata.hadooprpc.protocol.IUserLoginService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created by chendong on 2016/12/11.
 */
public class UserLoginAction {

    public static void main(String[] args) throws IOException {
        IUserLoginService userLoginService = RPC.getProxy(IUserLoginService.class, 100L
                , new InetSocketAddress("localhost", 8188), new Configuration());
        String login =userLoginService.login("chendong","123456");
        System.out.println(login);

    }

}
