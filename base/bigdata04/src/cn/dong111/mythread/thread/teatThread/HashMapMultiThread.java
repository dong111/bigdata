package cn.dong111.mythread.thread.teatThread;

import java.util.HashMap;
import java.util.Map;

/**
 * 并发下诡异的hashMap
 * @author chendong
 * @version [版本号, 2016-11-22]
 * @see [相关类/方法]
 * @since [产品/模块版本]
 */
public class HashMapMultiThread {

    static Map<String,String> map = new HashMap<String, String>();

    public static class AddThread implements Runnable{
        int start = 0;

        public AddThread(int start){
            this.start = start;
        }
        @Override
        public void run() {
            for(int i=start;i<100000;i+=2){
                map.put(Integer.toString(i),Integer.toBinaryString(i));
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        Thread t1 = new Thread(new HashMapMultiThread.AddThread(0));
        Thread t2 = new Thread(new HashMapMultiThread.AddThread(1));
        t1.start();
        t2.start();
        t1.join();t2.join();
        System.out.println(map.size());
    }


}
