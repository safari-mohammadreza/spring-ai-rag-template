package com.diaco.aranegar;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.reactive.config.EnableWebFlux;

@SpringBootApplication
@EnableWebFlux
@OpenAPIDefinition
public class AranegarApplication {

	public static void main(String[] args) {
		SpringApplication.run(AranegarApplication.class, args);
	}
}
