/**
 * 84116
 *2018年9月28日
 */
package hadoop01;

import org.apache.hadoop.ipc.RPC.Server;

import java.io.IOException;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
/**
 * @author 84116
 *
 */
public class RPCServer implements Bizable{
    
    /**
     * @param args
     * @throws IOException 
     * @throws HadoopIllegalArgumentException 
     */
    public static void main(String[] args) throws Exception {
        try {
            Server server = new RPC.Builder(new Configuration()).setProtocol(Bizable.class)
                    .setInstance(new RPCServer()).setBindAddress("192.168.8.100").setPort(2222).build();
        server.start();
        }catch(Exception e) {
            e.printStackTrace();
        }
        // TODO Auto-generated method stub
        
    }

    /* (non-Javadoc)
     * @see hadoop01.Bizable#sayHi(java.lang.String)
     */
    @Override
    public String sayHi(String name) {
        // TODO Auto-generated method stub
        return "Hi-"+name;
    }

}
