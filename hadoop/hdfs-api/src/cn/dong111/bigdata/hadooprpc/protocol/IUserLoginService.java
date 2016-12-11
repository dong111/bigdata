package cn.dong111.bigdata.hadooprpc.protocol;

/**
 * Created by chendong on 2016/12/11.
 */
public interface IUserLoginService {
    public static final long versionID = 100L;

    public String login(String name,String passwd);

}
