package com.baidu.hugegraph.example;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.util.LinkedHashMap;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.nodes.MappingNode;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.ScalarNode;

import dk.brics.automaton.RegExp;

public class k8sExample {

    private static String input = "/home/scorpiour/HugeGraph/hugegraph/.vscode/operator.yaml";
    private static String output = "/home/scorpiour/HugeGraph/hugegraph/.vscode/result.yaml";

    private static final String template = "hugegraph-computer-operator-system";
    private static final String namespace = "hugegraph-custom-system";

    private static final RegExp regex = new RegExp("\\shugegraph-computer-operator-system(\\r\\n|\\n)");


    private static void parseNode(Node node, int depth) {
        if (node instanceof ScalarNode) {
            ScalarNode snode = (ScalarNode)node;
            for(int i = 0; i < depth; i++) {
                System.out.print("-");
            }
            System.out.println(snode.getValue());
        } else if (node instanceof MappingNode) {
            MappingNode mnode = (MappingNode)node;
            mnode.getValue().forEach((tuple) -> {
                parseNode(tuple.getKeyNode(), depth + 1);
                parseNode(tuple.getValueNode(), depth + 1);
            });
            System.out.println("");
        }
    }

    private static void loadYaml(String path) {
        Yaml yaml = new Yaml();
        File file = new File(input);
        try {
            FileReader reader = new FileReader(file);
            int length = (int)(file.length());
            char buffer[] = new char[length];
            reader.read(buffer, 0, length);
            String content = new String(buffer);
            content = content.replaceAll(template, namespace);
            reader.close();

            File out = new File(output);
            FileWriter writer = new FileWriter(out);
            writer.write(content);
            writer.close();

            StringReader sr = new StringReader(content);



            Iterable<Node> nodeList = yaml.composeAll(sr);
            nodeList.forEach((node) -> {
                parseNode(node, 0);
            });

        } catch (FileNotFoundException e) {

        } catch (IOException e) {

        } finally {
            
        }
        System.out.println("finish!");
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) {
        loadYaml(input);
        
    }
}
