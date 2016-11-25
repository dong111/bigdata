package cn.dong111.mythread.pool;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.*;

/**
 * @author chendong
 * @version [版本号, 2016-11-25]
 * @see [相关类/方法]
 * @since [产品/模块版本]
 */
public class TestPool {


    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Future<?> submit = null;
        Random random = new Random();
        //创建调度下线程池
        ScheduledExecutorService exec = Executors.newScheduledThreadPool(4);

        //用来记录线程池的方法结果
        ArrayList<Future<?>> results = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
           submit = exec.schedule(new TaskCallable(i),random.nextInt(10), TimeUnit.HOURS);

            //存储线程执行结果
            results.add(submit);
        }

        Thread.sleep(1000);

        System.out.println(results.size());
        for(Future f:results){
            boolean done  = f.isDone();
            System.out.println(done?"已完成":"未完成");
            System.out.println("线程返回future结果："+f.get());
        }
        //exec.shutdown();

    }

}
