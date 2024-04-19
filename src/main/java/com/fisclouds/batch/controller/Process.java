package com.fisclouds.batch.controller;

import java.time.LocalDateTime;
import java.util.UUID;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;

@Entity
public class Process {
	
	@Id
	private UUID id;
	
	private String fileName;
	
	private String status;
	
	private LocalDateTime startDate;
	
	private LocalDateTime endDate;
	
	private String threadName;

	public UUID getId() {
		return id;
	}

	public void setId(UUID id) {
		this.id = id;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public LocalDateTime getStartDate() {
		return startDate;
	}

	public void setStartDate(LocalDateTime startDate) {
		this.startDate = startDate;
	}

	public LocalDateTime getEndDate() {
		return endDate;
	}

	public void setEndDate(LocalDateTime endDate) {
		this.endDate = endDate;
	}
	
	public void setThreadName(String threadName) {
		this.threadName = threadName;
	}
	
	public String getThreadName() {
		return threadName;
	}
	
}
