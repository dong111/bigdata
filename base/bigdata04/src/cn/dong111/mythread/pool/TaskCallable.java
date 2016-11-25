package cn.dong111.mythread.pool;

import java.util.Random;
import java.util.concurrent.Callable;

/**
 * @author chendong
 * @version [版本号, 2016-11-25]
 * @see [相关类/方法]
 * @since [产品/模块版本]
 */
public class TaskCallable implements Callable<String>{


    private int x;
    Random r = new Random();
    public TaskCallable(int x){this.x = x;}


    @Override
    public String call() throws Exception {
        String name = Thread.currentThread().getName();
        long currentTimeMillis = System.currentTimeMillis();
        System.out.println(name+"启动时间："+currentTimeMillis/1000);

        int rint = r.nextInt(3);

        Thread.sleep(rint*10);

        System.out.println(name+" is working……");

        return x+"";
    }
}
