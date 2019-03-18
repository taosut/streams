package vn.com.techcombank.push;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = {"vn.com.techcombank"})
public class PushApplication {

	public static void main(String[] args) {
		SpringApplication.run(PushApplication.class, args);
	}

}
