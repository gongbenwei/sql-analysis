package com.ledger.controller;


import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

/**
 * test controller
 */
@Controller
public class IndexController {
    @RequestMapping("/")
    public String index() {

        return "index";
    }
}
