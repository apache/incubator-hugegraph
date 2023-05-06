package org.apache.hugegraph.pd.rest;

import org.apache.hugegraph.pd.model.PromTargetsModel;
import org.apache.hugegraph.pd.service.PromTargetsService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * @author lynn.bond@hotmail.com on 2022/2/14
 */
@RestController
@Slf4j
@RequestMapping("/v1/prom")
public class PromTargetsAPI {

    @Autowired
    private PromTargetsService service;

    @GetMapping(value = "/targets/{appName}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<List<PromTargetsModel>> getPromTargets(@PathVariable(value = "appName", required = true) String appName) {
        return ResponseEntity.of(Optional.ofNullable(this.service.getTargets(appName)));
    }

    @GetMapping(value = "/targets-all", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<List<PromTargetsModel>> getPromAllTargets() {
        return ResponseEntity.of(Optional.ofNullable(this.service.getAllTargets()));
    }

    @GetMapping(value = "/demo/targets/{appName}", produces = MediaType.APPLICATION_JSON_VALUE)
    public List<PromTargetsModel> getDemoTargets(@PathVariable(value = "appName", required = true) String targetType) {

       PromTargetsModel model =null;
        switch (targetType) {
            case "node":
                model=PromTargetsModel.of()
                        .addTarget("10.14.139.26:8100")
                        .addTarget("10.14.139.27:8100")
                        .addTarget("10.14.139.28:8100")
                        .setMetricsPath("/metrics")
                        .setScheme("http");
                break;
            case "store":
                model=PromTargetsModel.of()
                        .addTarget("172.20.94.98:8521")
                        .addTarget("172.20.94.98:8522")
                        .addTarget("172.20.94.98:8523")
                        .setMetricsPath("/actuator/prometheus")
                        .setScheme("http");
                break;
            case "pd":
                model=PromTargetsModel.of()
                        .addTarget("172.20.94.98:8620")
                        .setMetricsPath("/actuator/prometheus");

                break;
            default:

        }

        return Collections.singletonList(model);
    }
}
