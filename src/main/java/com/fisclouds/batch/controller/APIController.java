package com.fisclouds.batch.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class APIController {
	
	@Autowired
	private ApiService apiService;
	
	@GetMapping("/api/v1/current")
	public List<ResponseData> currentProcess() {
		return apiService.current();
	}
	
	@GetMapping("/api/v2/current")
	public DataResponse currentProcess2() {
		return apiService.current2();
	}
	
	@GetMapping("/api/v1/history")
	public String history() {
		return null;
	}
}
