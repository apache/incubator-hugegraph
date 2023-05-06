package org.apache.hugegraph.pd.clitools;

import com.baidu.hugegraph.pd.client.PDClient;
import com.baidu.hugegraph.pd.client.PDConfig;
import com.baidu.hugegraph.pd.common.PDException;
import com.baidu.hugegraph.pd.grpc.Metapb;

public class Main {

    public static void main(String[] args) throws PDException {


        if ( args.length < 3){
            String error = " usage: pd-address config key[=value] \n key list: " +
                    "\n\tenableBatchLoad";
            System.out.println(error);
            System.exit(0);
        }
        String pd = args[0];
        String cmd = args[1];
        String param = args[2];
        System.out.println(pd + " " + cmd + " " + param);
        System.out.println("Result: \n");
        switch (cmd){
            case "config":
                doConfig(pd, param);
        }
    }

    public static void doConfig(String pd, String param) throws PDException {
        PDClient pdClient = PDClient.create(PDConfig.of(pd));
        String[] pair = param.split("=");
        String key = pair[0].trim();
        Object value = null;
        if ( pair.length > 1)
            value = pair[1].trim();
        if ( value == null){
            Metapb.PDConfig pdConfig = pdClient.getPDConfig();
            switch (key){
                case "enableBatchLoad":
                //    value = pdConfig.getEnableBatchLoad();
                    break;
                case "shardCount":
                    value = pdConfig.getShardCount();
                    break;
            }

            System.out.println("Get config " + key + "=" + value);
        }else{
            Metapb.PDConfig.Builder builder = Metapb.PDConfig.newBuilder();
            switch (key){
                case "enableBatchLoad":
                 //   builder.setEnableBatchLoad(Boolean.valueOf((String)value));
                case "shardCount":
                    builder.setShardCount(Integer.valueOf((String)value));
            }
            pdClient.setPDConfig(builder.build());
            System.out.println("Set config " + key + "=" + value);
        }
    }

}
