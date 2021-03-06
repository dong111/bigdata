package cn.dong111.mythread.thread.teatThread;

/**
 * @author chendong
 * @version [版本号, 2016-11-23]
 * @see [相关类/方法]
 * @since [产品/模块版本]
 * 通过实现Runable接口实现线程
 */
public class MyThreadWithImpliment implements Runnable {

    int x;

    public MyThreadWithImpliment(int x) {
        this.x = x;
    }

    @Override
    public void run() {
        String name = Thread.currentThread().getName();
        System.out.println("线程"+name+"的run方法被调用……");
        for (int i = 0; i < 10; i++) {
            System.out.println(name+"……"+i);
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        Thread thread1 = new Thread(new MyThreadWithImpliment(1),"thread-1");
        Thread thread2 = new Thread(new MyThreadWithImpliment(1),"thread-2");
        thread1.start();
        thread2.start();
        //注意调用run和调用start的区别，直接调用run，则都运行在main线程中
//        thread1.run();
//        thread2.run();

    }




}
