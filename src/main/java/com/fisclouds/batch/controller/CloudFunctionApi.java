package com.fisclouds.batch.controller;

import java.util.HashMap;
import java.util.Map;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class CloudFunctionApi {

	public void callAPI(String name) {
		
		RestTemplate restTemplate = new RestTemplate();
		
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		headers.setBearerAuth("eyJhbGciOiJSUzI1NiIsImtpZCI6IjkzNGE1ODE2NDY4Yjk1NzAzOTUzZDE0ZTlmMTVkZjVkMDlhNDAxZTQiLCJ0eXAiOiJKV1QifQ.eyJpc3MiOiJodHRwczovL2FjY291bnRzLmdvb2dsZS5jb20iLCJhenAiOiIzMjU1NTk0MDU1OS5hcHBzLmdvb2dsZXVzZXJjb250ZW50LmNvbSIsImF1ZCI6IjMyNTU1OTQwNTU5LmFwcHMuZ29vZ2xldXNlcmNvbnRlbnQuY29tIiwic3ViIjoiMTAyOTU3OTUwMTE5MTUyMjg1NTE4IiwiaGQiOiJmaXNjbG91ZHMuY29tIiwiZW1haWwiOiJhaW51bC55YXFpbkBmaXNjbG91ZHMuY29tIiwiZW1haWxfdmVyaWZpZWQiOnRydWUsImF0X2hhc2giOiJkZDBwWC1lVTM1MUxJbktmcW02ZEpRIiwiaWF0IjoxNzEyMTM5ODAyLCJleHAiOjE3MTIxNDM0MDJ9.lvDxMkX91nFKlxE2R12uJ5tNI5r3TPc9kLVjxrWVMvgBVUX6rtDf48j8Hd3YtLVVxVNseXCz5RM9iUt-QKCPy04WplNGbwUtAxsQ2zy3Cngllisxn2XxSTS2H8JfRxzdj_4x_4c0zNijlHjHZmzOhpDRf1IeewEvbVSPpHSaAfgRgjPbXtZupCEO4cRY70_n_Z-KSUrkPIgkTUuoFcJE7PB1bwrUKVcd_XYLGH_2z7v5YII3kgXbwToCgbbVCqbIBEp5e9KTU16k4_tN1ADfJezsyU5pBGfUQTMn7Af1g9eVOA4c-De3GfTaMD8JKTlBsecoXqaMqwtQpL9du1xWuw");
		
		Map<String, String> body = new HashMap<String, String>();
		body.put("name", name);
		HttpEntity<String> request = new HttpEntity<String>(body.toString(), headers);

		ResponseEntity<String> responseEntityStr = restTemplate.postForEntity("https://asia-southeast2-cdp-demo-395508.cloudfunctions.net/avro-to-bq", request, String.class);

		
	}
	
}
