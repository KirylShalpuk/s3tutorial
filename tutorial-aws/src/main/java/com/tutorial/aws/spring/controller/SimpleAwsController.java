package com.tutorial.aws.spring.controller;



import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.tutorial.aws.spring.data.DataObject;
import com.tutorial.aws.spring.service.SimpleAwsS3Service;






@RestController
@RequestMapping(value = "/javas3tutorialbucket")
public class SimpleAwsController {

	@Autowired
	SimpleAwsS3Service simpleAwsS3Service;
	
	@PostMapping("/addobject")
	public void createObject(@RequestBody DataObject dataObject) throws Exception {
		this.simpleAwsS3Service.uploadFile(dataObject);
	}
	
	@GetMapping("/fetchobject/{filename}")
	public void fetchObject(@PathVariable String filename) throws Exception {
		DataObject dataObject = new DataObject();
		dataObject.setName("filename");
		this.simpleAwsS3Service.downloadFile(dataObject);
	}
	
	@GetMapping("/listobjects")
	public List<String> listObjects() throws Exception {
		return this.simpleAwsS3Service.listObjects();
	}
	
	
	@PutMapping("/updateobject")
	public void updateObject(@RequestBody DataObject dataObject) throws Exception {
		this.simpleAwsS3Service.uploadFile(dataObject);
	}
	
	@DeleteMapping("/deleteobject")
	public void deleteObject(@RequestBody DataObject dataObject) {
		this.simpleAwsS3Service.deleteFile(dataObject);
	}	
	
	@PostMapping("/addbucket")
	public DataObject createBucket(@RequestBody DataObject dataObject) {
		return this.simpleAwsS3Service.addBucket(dataObject);
	}
	
	@GetMapping("/listbuckets")
	public List<String> listBuckets() {
		return this.simpleAwsS3Service.listBuckets();
	}
	
	@DeleteMapping("/deletebucket") 
	public void deleteBucket(@RequestBody DataObject dataObject) {
		this.simpleAwsS3Service.deleteBucket(dataObject.getName());
	}
	
	@DeleteMapping("/deletallbuckets")
	public void deleteAllBuckets() {
		this.simpleAwsS3Service.deleteAllBuckets();
	}
	
	
}
