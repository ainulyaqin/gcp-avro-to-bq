package com.fisclouds.batch.controller;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ApiService {

	@Autowired
	private ProcessRepository processRepository;
	
	public List<ResponseData> current(){
		
		List<ResponseData> data = new ArrayList<ResponseData>();
		
		Iterable<Process> prIterable =  processRepository.findAll();
		
		for(Process p : prIterable) {
			data.add(new ResponseData(p.getFileName(), p.getThreadName() , p.getStatus(),  p.getStartDate().toString()));
		}
		
		return data;
	}

	public DataResponse current2() {
		List<ResponseData> data = new ArrayList<ResponseData>();
		
		Iterable<Process> prIterable =  processRepository.findAll();
		
		for(Process p : prIterable) {
			data.add(new ResponseData(p.getFileName(), p.getThreadName() , p.getStatus(),  p.getStartDate().toString()));
		}
		
		return new DataResponse(data);
	}
	
}
