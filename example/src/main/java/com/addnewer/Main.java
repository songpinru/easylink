package com.addnewer;

/**
 * @author pinru
 * @version 1.0
 */
public class Main {
    public static void main(String[] args) throws Exception {
        System.out.printf("Hello and welcome!");
        Application.app(args).addComponent(ExampleConfig.class).addComponent(ExampleJob.class).run();
    }
}