package vn.com.techcombank.rest;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = {"vn.com.techcombank"})
public class RestApplication {

	public static void main(String[] args) {
		SpringApplication.run(RestApplication.class, args);
	}

}
