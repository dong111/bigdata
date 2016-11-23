package cn.dong111.mythread.thread.lock;

import java.util.ArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author chendong
 * @version [版本号, 2016-11-23]
 * @see [相关类/方法]
 * @since [产品/模块版本]
 */
public class MyLockTest {
    private static ArrayList<Integer> arrayList = new ArrayList<>();
    static Lock lock = new ReentrantLock();//注意这个地方
    public static void main(String[] args) {
        new Thread(){
            @Override
            public void run() {
                Thread thread = Thread.currentThread();
                lock.lock();

                try{
                    System.out.println(thread.getName()+"得到了锁");
                    for (int i = 0; i < 9; i++) {
                        arrayList.add(i);
                    }
                }catch (Exception e){

                }finally {
                    System.out.println(thread.getName()+"释放了锁");
                    lock.unlock();
                }
            }
        }.start();



        new Thread(){
            @Override
            public void run() {
                Thread thread = Thread.currentThread();
                lock.lock();

                try{
                    System.out.println(thread.getName()+"得到了锁");
                    for (int i = 0; i < 9; i++) {
                        arrayList.add(i);
                    }
                }catch (Exception e){

                }finally {
                    System.out.println(thread.getName()+"释放了锁");
                    lock.unlock();
                }
            }
        }.start();





    }

}
