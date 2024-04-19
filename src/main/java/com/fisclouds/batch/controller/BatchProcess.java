package com.fisclouds.batch.controller;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.opencsv.exceptions.CsvValidationException;

@Service
public class BatchProcess {
	
	private String path = "/Users/admin/Documents/app/testspliter/cimb_stage_file";

	@Autowired
	private ConcurrentProcess concurrentProcess;
	
	@Autowired
	private ProcessRepository processRepository;
	
	@Scheduled(cron = "0/10 * * ? * *")
	public void consumerQueueEmbeddingTask() throws IOException {
		
		/* list file */
		/* save to db */
		/* move file to final folder */
		
		Set<String> listFiles = listFilesUsingFilesList(path);
		
		for(String fileName : listFiles ) {
			try {
				if(!fileName.endsWith(".csv")) continue;
				
				if(processRepository.isExist(fileName)>0) continue;
				
				concurrentProcess.process(fileName,path);
			} catch (CsvValidationException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public Set<String> listFilesUsingFilesList(String dir) throws IOException {
	    try (Stream<Path> stream = Files.list(Paths.get(dir))) {
	        return stream
	          .filter(file -> !Files.isDirectory(file))
	          .map(Path::getFileName)
	          .map(Path::toString)
	          .collect(Collectors.toSet());
	    }
	}
	
	

}
