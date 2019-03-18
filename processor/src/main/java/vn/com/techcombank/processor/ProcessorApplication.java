package vn.com.techcombank.processor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = { "vn.com.techcombank" })
public class ProcessorApplication {

	public static void main(String[] args) {
		SpringApplication.run(ProcessorApplication.class, args);
	}

}
