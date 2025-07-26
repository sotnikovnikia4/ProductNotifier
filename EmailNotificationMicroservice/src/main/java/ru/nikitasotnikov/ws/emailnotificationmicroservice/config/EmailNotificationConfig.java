package ru.nikitasotnikov.ws.emailnotificationmicroservice.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

@Configuration
public class EmailNotificationConfig {
    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }
}
