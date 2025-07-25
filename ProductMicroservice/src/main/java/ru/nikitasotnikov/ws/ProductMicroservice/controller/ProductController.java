package ru.nikitasotnikov.ws.ProductMicroservice.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.nikitasotnikov.ws.ProductMicroservice.service.ProductService;
import ru.nikitasotnikov.ws.ProductMicroservice.service.dto.CreateProductDto;

import java.util.Date;

@RestController
@RequestMapping("/product")
public class ProductController {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final ProductService productService;

    public ProductController(ProductService productService) {
        this.productService = productService;
    }

    @PostMapping
    public ResponseEntity<Object> createProduct(@RequestBody CreateProductDto createProductDto){
        String productId = null;
        try {
            productId = productService.createProduct(createProductDto);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(new ErrorMessage(new Date(), e.getMessage()));
        }
        return ResponseEntity.status(HttpStatus.CREATED).body(productId);
    }
}
