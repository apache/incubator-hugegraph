package org.apache.hugegraph.pd.cli;

import org.apache.hugegraph.pd.cli.cmd.ChangeRaft;
import org.apache.hugegraph.pd.cli.cmd.Command;
import org.apache.hugegraph.pd.cli.cmd.Config;
import org.apache.hugegraph.pd.cli.cmd.Parameter;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CliApplication {

    public static void main(String[] args) {
        try {
            Parameter parameter = Command.toParameter(args);
            Command command;
            switch (parameter.getCmd()) {
                case "config":
                    command = new Config(parameter.getPd());
                    break;
                case "change_raft":
                    command = new ChangeRaft(parameter.getPd());
                    break;
//                case "check_peers":
//                    command = new CheckPeers(parameter.getPd());
//                    break;
                default:
                    log.error("无效的指令");
                    return;
            }
            command.action(parameter.getParams());
        } catch (Exception e) {
            log.error("main thread error:", e);
            System.exit(0);
        } finally {

        }

    }
}
