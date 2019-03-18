package vn.com.techcombank.clientapp;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import vn.com.techcombank.consumer.Receiver;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.Banner;

import static java.lang.System.exit;

import java.util.Scanner;

@SpringBootApplication(scanBasePackages = { "vn.com.techcombank" })
public class ClientAppApplication implements CommandLineRunner {

	public static void main(String[] args) {
		SpringApplication app = new SpringApplication(ClientAppApplication.class);
		app.setBannerMode(Banner.Mode.OFF);
		app.run(args);
	}

	@Override
	public void run(String... args) throws Exception {
//		System.out.println("Starting Client-app ...");
//
//		Scanner scanner = new Scanner(System.in);
//		
//		while(true)
//		{
//			System.out.println("Press q to exit ...");
//			String input = scanner.nextLine();
//			
//			if("q".equals(input))
//				break;
//		}
//		
//		scanner.close();
	}

}
