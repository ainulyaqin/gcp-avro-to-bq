package com.fisclouds.batch.controller;

import java.io.IOException;
import java.nio.file.Paths;

import org.springframework.stereotype.Service;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

@Service
public class UploadToGCS {

	private static String projectId = "cdp-demo-395508";
	private static String bucketName = "bkt-cdp-platform-data-sources";
	
	
	public void uploadFile(String fileName, String path) throws IOException {
	    // Create a new GCS client
	    Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
	    // The blob ID identifies the newly created blob, which consists of a bucket name and an object
	    // name
	    String filePath = path+"/"+fileName;
	    BlobId blobId = BlobId.of(bucketName, fileName);
	    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();

	    // The filepath on our local machine that we want to upload
	    

	    // upload the file and print the status
	    storage.createFrom(blobInfo, Paths.get(filePath));
	    System.out
	        .println("File " + filePath + " uploaded to bucket " + bucketName );
	  }

}
