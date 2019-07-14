package com.tw.td

import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RestController

@RestController
class Router {

    @GetMapping("/")
    fun healthcheck(): String = "Messaging endpoint is listening !"

}
