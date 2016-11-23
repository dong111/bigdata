package cn.dong111.mythread.thread.lock;

import java.util.ArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author chendong
 * @version [版本号, 2016-11-23]
 * @see [相关类/方法]
 * @since [产品/模块版本]
 *
 * 观察现象：一个线程获得锁后，另一个线程取不到锁，不会一直等
 *
 */
public class MyTryLock {

    private static ArrayList<Integer> arrayList = new ArrayList<>();
    static Lock lock = new ReentrantLock();//Lock接口的实现类

    public static void main(String[] args) {
        new Thread(){
            @Override
            public void run() {
                Thread thread = Thread.currentThread();
                boolean tryLock = lock.tryLock();
                if(tryLock){
                    try {
                        System.out.println(thread.getName()+"得到了锁");
                        Thread.sleep(1000);
                        for (int i = 0; i < 5; i++) {
                            arrayList.add(i);
                        }
                    }catch (Exception e){
                        e.printStackTrace();
                    }finally {
                        System.out.println(thread.getName()+"释放了锁");
                        lock.unlock();
                    }
                }else {
                    System.out.println(thread.getName()+"没有得到锁");
                }
            }
        }.start();


        new Thread(){
            @Override
            public void run() {
                Thread thread = Thread.currentThread();
                boolean tryLock = lock.tryLock();
                if(tryLock){
                    try {
                        System.out.println(thread.getName()+"得到了锁");
                        for (int i = 0; i < 5; i++) {
                            arrayList.add(i);
                        }
                    }catch (Exception e){
                        e.printStackTrace();
                    }finally {
                        System.out.println(thread.getName()+"释放了锁");
                        lock.unlock();
                    }
                }else {
                    System.out.println(thread.getName()+"没有得到锁");
                }
            }
        }.start();

    }

}
