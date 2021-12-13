package com.baidu.hugegraph.k8s;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.MapConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import com.baidu.hugegraph.computer.k8s.driver.KubernetesDriver;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.OptionSpace;
import com.baidu.hugegraph.util.Log;

public class K8sDriverProxy {

    private static final Logger LOG = Log.logger(K8sDriverProxy.class);

    private static final String USER_DIR = System.getProperty("user.dir");

    private static boolean K8S_API_ENABLED = false;

    private static String NAMESPACE = "";
    private static String KUBE_CONFIG_PATH = "";
    private static String HUGEGRAPH_URL = "";
    private static String ENABLE_INTERNAL_ALGORITHM = "";
    private static String INTERNAL_ALGORITHM_IMAGE_URL = "";
    private static Map<String, String> ALGORITHM_PARAMS = null;
    private static String INTERNAL_ALGORITHM = "[]";

    protected HugeConfig config;
    protected static KubernetesDriver driver;

    static {
        OptionSpace.register("computer-driver",
                             "com.baidu.hugegraph.computer.driver.config" +
                             ".ComputerOptions");
        OptionSpace.register("computer-k8s-driver",
                             "com.baidu.hugegraph.computer.k8s.config" +
                             ".KubeDriverOptions");
        OptionSpace.register("computer-k8s-spec",
                             "com.baidu.hugegraph.computer.k8s.config" +
                             ".KubeSpecOptions");
    }

    public static void disable() {
        K8S_API_ENABLED = false;
    }

    public static void setConfig(String namespace, String kubeConfigPath,
                                 String hugegraphUrl,
                                 String enableInternalAlgorithm,
                                 String internalAlgorithmImageUrl,
                                 String internalAlgorithm,
                                 Map<String, String> algorithms)
                                 throws IOException {
        File kubeConfigFile;
        if (!kubeConfigPath.startsWith("/")) {
            kubeConfigFile = new File(USER_DIR + "/" + kubeConfigPath);
        } else {
            kubeConfigFile = new File(kubeConfigPath);
        }
        if (!kubeConfigFile.exists() || StringUtils.isEmpty(hugegraphUrl)) {
            throw new IOException("[K8s API] k8s config fail");
        }

        K8S_API_ENABLED = true;
        NAMESPACE = namespace;
        KUBE_CONFIG_PATH = kubeConfigFile.getAbsolutePath();
        HUGEGRAPH_URL = hugegraphUrl;
        ENABLE_INTERNAL_ALGORITHM = enableInternalAlgorithm;
        INTERNAL_ALGORITHM_IMAGE_URL = internalAlgorithmImageUrl;
        ALGORITHM_PARAMS = algorithms;
        INTERNAL_ALGORITHM = internalAlgorithm;
    }

    public static boolean isK8sApiEnabled() {
        return K8S_API_ENABLED;
    }

    public static boolean isValidAlgorithm(String algorithm) {
        return ALGORITHM_PARAMS.containsKey(algorithm);
    }

    public static String getAlgorithmClass(String algorithm) {
        return ALGORITHM_PARAMS.get(algorithm);
    }

    public K8sDriverProxy(String partitionsCount, String algorithm) {
        try {
            if (!K8sDriverProxy.K8S_API_ENABLED) {
                throw new UnsupportedOperationException(
                      "The k8s api not enabled.");
            }
            String paramsClass = ALGORITHM_PARAMS.get(algorithm);
            this.initConfig(partitionsCount, INTERNAL_ALGORITHM, paramsClass);
            this.initK8sDriver();
        } catch (Throwable throwable) {
            LOG.error("Failed to start K8sDriverProxy ", throwable);
        }
    }

    protected void initConfig(String partitionsCount,
                              String internalAlgorithm,
                              String paramsClass) {
        HashMap<String, String> options = new HashMap<>();

        // from configuration
        options.put("k8s.namespace", K8sDriverProxy.NAMESPACE);
        options.put("k8s.kube_config", K8sDriverProxy.KUBE_CONFIG_PATH);
        options.put("hugegraph.url", K8sDriverProxy.HUGEGRAPH_URL);
        options.put("k8s.enable_internal_algorithm",
                    K8sDriverProxy.ENABLE_INTERNAL_ALGORITHM);
        options.put("k8s.internal_algorithm_image_url",
                    K8sDriverProxy.INTERNAL_ALGORITHM_IMAGE_URL);

        // from rest api params
        // partitionsCount >= worker_instances
        options.put("job.partitions_count", partitionsCount);
        options.put("k8s.internal_algorithm", internalAlgorithm);
        options.put("algorithm.params_class", paramsClass);
        MapConfiguration mapConfig = new MapConfiguration(options);
        this.config = new HugeConfig(mapConfig);
    }

    /**
     * Reuse K8s driver for task to operate, init only if it was null
     * TODO: use singleton for it
     */
    private void initK8sDriver() {
        if (driver == null) {
            driver = new KubernetesDriver(this.config);
        }
    }

    public KubernetesDriver getK8sDriver() {
        return driver;
    }

    public void close() {
        // TODO: Comment now & delete this method after ensuring it
        //driver.close();
    }
}
