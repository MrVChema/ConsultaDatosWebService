package com.data.GrupoCuatroS.application;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "com.data.GrupoCuatroS")
public class BigQueryWebServiceApplication {

	public static void main(String[] args) {
        SpringApplication.run(BigQueryWebServiceApplication.class, args);
    }

}
