package core.raft;

import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.core.StateMachineAdapter;

public class MockStateMachine extends StateMachineAdapter {
    @Override
    public void onApply(Iterator iter) {
        while (iter.hasNext()){
            iter.next();
        }
    }
}
