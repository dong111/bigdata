package cn.dong111.mythread.thread.lock;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author chendong
 * @version [版本号, 2016-11-23]
 * @see [相关类/方法]
 * @since [产品/模块版本]
 */
public class MyInterruptibly {
    private Lock lock = new ReentrantLock();

    public static void main(String[] args) {
        MyInterruptibly test = new MyInterruptibly();
        MyThread thread1 = new MyThread(test);
        MyThread thread2 = new MyThread(test);
        thread1.start();
        thread2.start();

        try {
            Thread.sleep(2000);
        }catch (InterruptedException e){
            e.printStackTrace();
        }

        //注意:当一个线程获取锁后，是不会被interrupt方法中断的
        //thread1.interrupt();
        thread2.interrupt();
        System.out.println("===========================");

    }


    //获取锁操作  如果获取成功继续执行方法体，否者抛出异常中断方法执行
    public void insert(Thread thread) throws InterruptedException {
        lock.lockInterruptibly();//注意 如果需要正确中断等待锁的线程
        // 必须将获取锁放在外面，然后将InterrupedException抛出

        try {
            System.out.println(thread.getName()+"得到了锁");
            Thread.sleep(10000);
        }
        finally {
            System.out.println(Thread.currentThread().getName()+"执行了finally");
            System.out.println(thread.getName()+"释放了锁");
            lock.unlock();
        }

    }
}





//定义个线程实现类
class MyThread extends Thread{
    private MyInterruptibly test = null;

    public MyThread(MyInterruptibly test) {
        this.test = test;
    }

    @Override
    public void run() {
        System.out.println(Thread.currentThread().getName()+"-->run");
        try {
            test.insert(Thread.currentThread());
        } catch (InterruptedException e) {
            System.out.println(Thread.currentThread().getName()+"被中断了");
            e.printStackTrace();
        }
    }
}
