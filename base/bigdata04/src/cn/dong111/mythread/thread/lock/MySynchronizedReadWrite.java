package cn.dong111.mythread.thread.lock;

/**
 * @author chendong
 * @version [版本号, 2016-11-23]
 * @see [相关类/方法]
 * @since [产品/模块版本]
 */
public class MySynchronizedReadWrite {

    public static void main(String[] args) {
        final MySynchronizedReadWrite test = new MySynchronizedReadWrite();

        new Thread(){
            @Override
            public void run() {
               test.get(Thread.currentThread());
            }
        }.start();

        new Thread(){
            @Override
            public void run() {
                test.get(Thread.currentThread());
            }
        }.start();

    }







    public synchronized void get(Thread thread){
        long start = System.currentTimeMillis();
        int i=0;
        while (System.currentTimeMillis()-start<=1){
            i++;
            if(i%4==0){
                System.out.println(thread.getName()+"正在进行写操作");
            }else{
                System.out.println(thread.getName()+"正在进行读操作");
            }
        }
        System.out.println(thread.getName()+"读写操作完毕");
    }

}
