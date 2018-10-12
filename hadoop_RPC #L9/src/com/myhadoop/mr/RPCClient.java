/**
 * 84116
 *2018�?9�?29�?
 */
package com.myhadoop.mr;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

/**
 * @author 84116
 *
 */
public class RPCClient {

    /**
     * @param args
     * @throws IOException 
     */
    public static void main(String[] args)  {
        // TODO Auto-generated method stub
        try {
            Bizable proxy = RPC.getProxy(Bizable.class, 10010, new InetSocketAddress("192.168.8.100",2222),new Configuration());
            String result = proxy.sayHi("Tom");
            System.out.println(result);
            RPC.stopProxy(proxy);
        }catch(Exception e) {
            e.printStackTrace();
        }
        
    }

}
