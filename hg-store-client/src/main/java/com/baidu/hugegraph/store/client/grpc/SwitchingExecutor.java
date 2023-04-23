package com.baidu.hugegraph.store.client.grpc;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * @author lynn.bond@hotmail.com on 2021/12/1
 */
@ThreadSafe
final class SwitchingExecutor {

    static SwitchingExecutor of(){
        return new SwitchingExecutor();
    }

    private SwitchingExecutor(){}

    <T> Optional<T>  invoke(Supplier<Boolean> switcher,Supplier<T> trueSupplier,Supplier<T> falseSupplier){
        Optional<T> option = null;

        if(switcher.get()){
            option= Optional.of(trueSupplier.get());
        }else{
            option=Optional.of(falseSupplier.get());
        }
        if (option == null) {
            option = Optional.empty();
        }

        return option;
    }
}
