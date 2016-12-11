package cn.dong111.bigdata.hadooprpc.service;

import cn.dong111.bigdata.hadooprpc.protocol.IUserLoginService;

/**
 * Created by chendong on 2016/12/11.
 */
public class UserLoginServiceImpl implements IUserLoginService {
    @Override
    public String login(String name, String passwd) {
        return name+"logged in successfully……";
    }
}
