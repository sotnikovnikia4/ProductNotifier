package ru.nikitasotnikov.ws.emailnotificationmicroservice.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;
import ru.nikitasotnikov.ws.core.ProductCreatedEvent;
import ru.nikitasotnikov.ws.emailnotificationmicroservice.exception.NonRetryableException;
import ru.nikitasotnikov.ws.emailnotificationmicroservice.exception.RetryableException;

@Component
@KafkaListener(topics = "product-created-events-topic")
public class ProductCreatedEventHandler {
    private static final Logger log = LoggerFactory.getLogger(ProductCreatedEventHandler.class);

    private final RestTemplate restTemplate;

    @Autowired
    public ProductCreatedEventHandler(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    @KafkaHandler
    public void handle(ProductCreatedEvent productCreatedEvent){
        log.info("Received event: {}, productId {}", productCreatedEvent.getTitle(), productCreatedEvent.getProductId());

        String url = "http://localhost:8090/response/200";
        try{
            ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.GET, null, String.class);
            if(response.getStatusCode().value() == HttpStatus.OK.value()) {
                log.info("Received response: {}", response.getBody());
            }
        }
        catch(ResourceAccessException e){
            log.error(e.getMessage());
            throw new RetryableException(e);
        }
        catch(HttpServerErrorException e){
            log.error(e.getMessage());
            throw new NonRetryableException(e);
        }
        catch(Exception e){
            log.error(e.getMessage(), e);
            throw new NonRetryableException(e);
        }
    }
}
