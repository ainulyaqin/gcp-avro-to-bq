package com.fisclouds.batch.controller;

import java.time.LocalDateTime;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

@Controller("/")
public class HomeController {
	
	@GetMapping()
	public String greeting(Model model) {
		model.addAttribute("currentDate", LocalDateTime.now().toString());
		return "home";
	}

	
}
