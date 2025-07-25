package ru.nikitasotnikov.ws.ProductMicroservice.service;

import ru.nikitasotnikov.ws.ProductMicroservice.service.dto.CreateProductDto;

import java.util.concurrent.ExecutionException;

public interface ProductService {
    String createProduct(CreateProductDto createProductDto) throws ExecutionException, InterruptedException;
}
