package com.tutorial.aws.spring.application;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;


@SpringBootApplication
@ComponentScan({ "com.tutorial.aws.spring" })
public class SimpleAwsClient {

    public static void main(String[] args) {
        SpringApplication.run(SimpleAwsClient.class, args);
    }
}
