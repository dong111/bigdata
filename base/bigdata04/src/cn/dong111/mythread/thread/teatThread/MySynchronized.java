package cn.dong111.mythread.thread.teatThread;

/**
 * @author chendong
 * @version [版本号, 2016-11-22]
 * @see [相关类/方法]
 * @since [产品/模块版本]
 *
 * 如果一个代码库被synchronized修饰了，当一个线程获取了对应的锁，并且执行该代码块时，
 * 其它线程只能一直等待，等待获取锁的线程释放，如果这里要让获取锁的线程释放锁只会有两种情况
 * 1.获取锁的线程执行完这段代码，然后线程释放对锁的占有
 * 2.线程执行发生异常，此时jvm会让线程自动释放锁
 *
 */
public class MySynchronized
{
    public static void main(String[] args) {
        final  MySynchronized mySynchronized = new MySynchronized();
        final MySynchronized mySynchronized2 = new MySynchronized();
        new Thread("thread1"){
            @Override
            public void run() {
                synchronized (mySynchronized){
                    System.out.println(this.getName()+"start");
                    int i = 1/0;//如果发生异常，jvm将会释放锁
                    try {
                        Thread.sleep(5000);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    System.out.println(this.getName()+"醒来了");
                    System.out.println(this.getName()+"end");
                }
            }
        }.start();


        new Thread("thread2"){
            @Override
            public void run() {
                synchronized (mySynchronized){//争抢同一把锁时候，线程1没有释放之前，线程2只能等待
//                    synchronized (mySynchronized2){//如果不是同一把锁，可以看到两个
                    System.out.println(this.getName()+"start");
                    System.out.println(this.getName()+"end");
                }
            }
        }.start();





    }

}
