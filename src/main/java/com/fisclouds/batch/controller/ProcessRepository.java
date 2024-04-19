package com.fisclouds.batch.controller;

import java.util.UUID;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ProcessRepository extends CrudRepository<Process, UUID> {

	@Modifying
	@Query(value ="update process set status=:status where file_name=:fileName", 
			  nativeQuery = true)
	public int update(String fileName, String status);
	
	@Query(value ="select count(1) from process where file_name=:fileName", nativeQuery = true)
	public long isExist(String fileName);
	
}
