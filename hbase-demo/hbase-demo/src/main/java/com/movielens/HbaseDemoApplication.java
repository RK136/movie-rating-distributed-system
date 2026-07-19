package com.movielens;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class HbaseDemoApplication {
    public static void main(String[] args) {
        SpringApplication.run(HbaseDemoApplication.class, args);
        System.out.println("✅ 后端启动成功！");
    }
}