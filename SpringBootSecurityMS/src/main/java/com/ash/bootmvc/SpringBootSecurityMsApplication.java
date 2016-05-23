package com.ash.bootmvc;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

//@ComponentScan(basePackages="com.ash.bootmvc")
@SpringBootApplication
public class SpringBootSecurityMsApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringBootSecurityMsApplication.class, args);
	}
}
