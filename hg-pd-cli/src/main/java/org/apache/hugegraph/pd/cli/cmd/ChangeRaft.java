package org.apache.hugegraph.pd.cli.cmd;

import org.apache.hugegraph.pd.common.PDException;

/**
 * @author zhangyingjie
 * @date 2023/10/17
 **/
public class ChangeRaft extends Command {

    public ChangeRaft(String pd) {
        super(pd);
    }

    @Override
    public void action(String[] params) throws PDException {
        pdClient.updatePdRaft(params[0]);
    }
}
