package com.data.GrupoCuatroS.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class HelloWorldController {

    @GetMapping("/hola")
    public String holaMundo() {
        return "¡Hola Mundo! Tu webservice está funcionando correctamente.";
    }
}