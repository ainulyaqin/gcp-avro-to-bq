package com.fisclouds.batch.controller;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

@Service
public class ConcurrentProcess {
	private Log log = LogFactory.getLog(ConcurrentProcess.class);

	@Autowired
	private ProcessRepository processRepository;

	public final  String STARTING = "starting";
	public final  String COMPLETE = "complete";
	public final  String ERROR = "error";

	public  final String[] fieldEncryption = new String[] {"ip_address","email","cc"};
 	
	@Autowired
	private UploadToGCS uploadToGCS;
	
	@Autowired
	private CloudFunctionApi cloudFunctionApi;
	
	@Async
	public void process(String filename, String pathFile) throws CsvValidationException, IOException {
		String threadName = Thread.currentThread().getName();

		/*
		 * save to db
		 */
		Process p = save(filename, threadName);
		File file = new File(pathFile + "/" + filename);

		/*
		 * convert file
		 */
		convertToAvroX(pathFile, filename);

		/*
		 * upload to gcs
		 */
		uploadToGCS.uploadFile(filename.replace(".csv", ".avro"), pathFile);

		/*
		 * move file
		 */
		//moveFile(file, pathFile);
		
		/*
		 * call api
		 */
		cloudFunctionApi.callAPI(filename.replace(".csv", ".avro"));
		
		p.setStatus(COMPLETE);
		
		try {
			Thread.sleep(30000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		processRepository.save(p);

	}

	private  void convertToAvroX(String pathFile, String filename) throws CsvValidationException, IOException {
		// Replace with your actual file paths
		String csvFilePath = pathFile + "/" + filename;
		String avroFilePath = pathFile + "/"+filename.replace(".csv", ".avro");
		String avroSchema = """
					    		{
				  "type": "record",
				  "name": "User",
				  "fields": [
				    {
				      "name": "registration_dttm",
				      "type": [
				        "null",
				        "string"
				      ]
				    },
				    {
				      "name": "id",
				      "type": "string"
				    },
				    {
				      "name": "first_name",
				      "type": [
				        "null",
				        "string"
				      ]
				    },
				    {
				      "name": "last_name",
				      "type": "string"
				    },
				    {
				      "name": "email",
				      "type": [
				        "null",
				        "string"
				      ]
				    },
				    {
				      "name": "gender",
				      "type": [
				        "null",
				        "string"
				      ]
				    },
				    {
				      "name": "ip_address",
				      "type": "string"
				    },
				    {
				      "name": "cc",
				      "type": [
				        "null",
				        "string"
				      ]
				    },
				    {
				      "name": "country",
				      "type": "string"
				    },
				    {
				      "name": "birthdate",
				      "type": [
				        "null",
				        "string"
				      ]
				    },
				    {
				      "name": "salary",
				      "type": [
				        "null",
				        "string"
				      ]
				    },
				    {
				      "name": "title",
				      "type": [
				        "null",
				        "string"
				      ]
				    },
				    {
				      "name": "comments",
				      "type": [
				        "null",
				        "string"
				      ]
				    }
				  ]
				}
					    		""";

		Schema schema = new Schema.Parser().parse(avroSchema);
		
		// Parse CSV and convert to Avro records
		List<GenericRecord> avroRecords = convertCsvToAvro(csvFilePath, schema);

		// Write Avro records to file
		writeAvroToFile(avroRecords, avroFilePath,schema);

		System.out.println("CSV to Avro conversion successful!");
	}

	private void moveFile(File file, String pathFile) {
		String dest = pathFile + "/final/" + file.getName();
		file.renameTo(new File(dest));
	}

	private Process save(String filename, String threadname) {
		Process p = new Process();
		p.setThreadName(threadname);
		p.setId(UUID.randomUUID());
		p.setFileName(filename);
		p.setStatus(STARTING);
		p.setStartDate(LocalDateTime.now());
		
		return processRepository.save(p);
	}
	
	private void update(String filename) {
		processRepository.update(filename, COMPLETE);
	}

	private  List<GenericRecord> convertCsvToAvro(String csvFilePath, Schema schema)
			throws IOException, CsvValidationException {
		List<GenericRecord> avroRecords = new ArrayList<>();
		

		try (CSVReader reader = new CSVReader(new FileReader(csvFilePath))) {
			String[] header = reader.readNext(); // Assuming a header row

			// Map header names to Avro schema field names (adjust if necessary)
			Map<String, String> headerMapping = new HashMap<>();
			for (int i = 0; i < header.length; i++) {
				headerMapping.put(header[i].toLowerCase(), schema.getFields().get(i).name());
			}

			String[] nextLine;
			while ((nextLine = reader.readNext()) != null) {
		        
				GenericRecord record = new Record(schema);
				//Record record = new GenericRecordBuilder(schema).build();
				for (int i = 0; i < nextLine.length; i++) {
					String fieldName = headerMapping.get(header[i].toLowerCase());
					if (fieldName != null) {
						//record.put(fieldName, nextLine[i]);
						record.put(fieldName, encryptionValue(fieldName, nextLine[i]) );
					}
				}
				avroRecords.add(record);
			}
		}
		return avroRecords;
	}

	private  String encryptionValue(String fieldName, String value) {
		
		List<String> arr = Arrays.asList(fieldEncryption);
		
		return arr.contains(fieldName) ? AESUtil.encrypt(value):value;
	}
	
	private  void writeAvroToFile(List<GenericRecord> avroRecords, String avroFilePath,Schema schema ) throws IOException {

		GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
		dataFileWriter.create(avroRecords.get(0).getSchema(), new File(avroFilePath));
		for (GenericRecord record : avroRecords) {
			dataFileWriter.append(record);
		}
		dataFileWriter.close();
	}

}
