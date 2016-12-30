package cn.dong111.bigdata.mapreduce.logenhance;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;

/**
 * Created by chendong on 2016/12/29.
 */
public class DBLoader {

    public static  void dbLoader(Map<String,String> ruleMap) throws Exception{
        Connection conn = null;
        Statement st = null;
        ResultSet res = null;


        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://121.40.104.90:3306/test", "root", "dbRoot258");
            st = conn.createStatement();
            res = st.executeQuery("select url,content from url_rule");
            while (res.next()) {
                ruleMap.put(res.getString(1), res.getString(2));
            }

        } finally {
            try{
                if(res!=null){
                    res.close();
                }
                if(st!=null){
                    st.close();
                }
                if(conn!=null){
                    conn.close();
                }

            }catch(Exception e){
                e.printStackTrace();
            }
        }
    }

}
