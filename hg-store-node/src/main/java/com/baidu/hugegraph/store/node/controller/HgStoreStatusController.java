package com.baidu.hugegraph.store.node.controller;

import java.io.Serializable;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.baidu.hugegraph.store.grpc.state.ScanState;
import com.baidu.hugegraph.store.node.entry.RestResult;
import com.baidu.hugegraph.store.node.grpc.HgStoreNodeState;
import com.baidu.hugegraph.store.node.grpc.HgStoreStreamImpl;
import com.baidu.hugegraph.store.node.model.HgNodeStatus;
import com.google.protobuf.util.JsonFormat;

/**
 * @author lynn.bond@hotmail.com created on 2021/11/1
 */
@RestController
public class HgStoreStatusController {

    @Autowired
    HgStoreStreamImpl streamImpl;

    @GetMapping("/-/echo")
    public HgNodeStatus greeting(@RequestParam(value = "name", defaultValue = "World") String name) {
        return new HgNodeStatus(0, name + " is ok.");
    }

    @GetMapping("/-/state")
    public HgNodeStatus getState() {
        return new HgNodeStatus(0, HgStoreNodeState.getState().name());
    }

    @PutMapping("/-/state")
    public HgNodeStatus setState(@RequestParam(value = "name") String name) {

        switch (name) {
            case "starting":
                HgStoreNodeState.goStarting();
                break;
            case "online":
                HgStoreNodeState.goOnline();
                break;
            case "stopping":
                HgStoreNodeState.goStopping();
                break;
            default:
                return new HgNodeStatus(1000, "invalid parameter: " + name);
        }

        return new HgNodeStatus(0, name);
    }

    @GetMapping(value = "/-/scan",
                produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public Serializable getScanState() {
        RestResult result = new RestResult();
        try {
            ScanState state = streamImpl.getState();
            JsonFormat.Printer printer = JsonFormat.printer();
            printer = printer.includingDefaultValueFields().preservingProtoFieldNames();
            return printer.print(state);
        } catch (Exception e) {
            result.setState(RestResult.ERR);
            result.setMessage(e.getMessage());
            return result;
        }
    }

}
