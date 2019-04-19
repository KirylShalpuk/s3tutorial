package com.tutorial.aws.spring.service;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;

import com.tutorial.aws.spring.data.DataObject;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;


@Service
public class SimpleAwsS3Service {
	

@Value("${cloud.aws.credentials.accessKey}")
private String key;

@Value("${cloud.aws.credentials.secretKey}")
private String secretKey;
	
private S3Client s3Client;

@PostConstruct
 public void initialize() {
	BasicAWSCredentials awsCreds = new BasicAWSCredentials("access_key_id", "secret_key_id");
	AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
	                        .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
	                        .build();
	s3Client = S3Client.builder()
	                      .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
	                      .region(Region.US_EAST_1).build();
	 
 }


	public void uploadFile(DataObject dataObject) throws S3Exception, AwsServiceException, SdkClientException, URISyntaxException, FileNotFoundException {

		
			PutObjectRequest putObjectRequest = PutObjectRequest.builder().bucket("javas3tutorial").key(dataObject.getName()).acl(ObjectCannedACL.PUBLIC_READ).
					build();
			
			
			File file = new File(
					getClass().getClassLoader().getResource(dataObject.getName()).getFile()
				);
			
			s3Client.putObject(putObjectRequest, RequestBody.fromFile(file));
			
	}
	
	public void downloadFile(DataObject dataObject) throws NoSuchKeyException, S3Exception, AwsServiceException, SdkClientException, IOException
	{
		GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket("javas3tutorial").key("sample.png").build();
		Resource resource = new ClassPathResource(".");
		

		s3Client.getObject(getObjectRequest, Paths.get(resource.getURL().getPath()+"/test.png"));
		
		
	}
	
	public List<String> listObjects() {
		return this.listObjects("javas3tutorial");
	}
	
	public List<String> listObjects(String name) {
		List<String> names = new ArrayList<>();
		ListObjectsRequest listObjectsRequest = ListObjectsRequest.builder().bucket(name).build();
		ListObjectsResponse listObjectsResponse = s3Client.listObjects(listObjectsRequest);
		listObjectsResponse.contents().stream().forEach(x -> names.add(x.key()));
		return names;
	}
	
	public void deleteFile(DataObject dataObject) {
		this.deleteFile("javas3tutorial", dataObject.getName());		
	}
	
	public void deleteFile(String bucketName, String fileName) {
		DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder().bucket(bucketName).key(fileName).build();
		s3Client.deleteObject(deleteObjectRequest);
	}
	
	
	public void deleteBucket(String bucket) {
		
		List<String> keys = this.listObjects(bucket);
		List<ObjectIdentifier> identifiers = new ArrayList<>();
		int iteration = 0;
		for(String key : keys) {
			
			ObjectIdentifier objIdentifier = ObjectIdentifier.builder().key(key).build();
		  identifiers.add(objIdentifier);
		  iteration++;
			
			if(iteration == 3){
				iteration = 0;
				DeleteObjectsRequest deleteObjectsRequest = DeleteObjectsRequest.builder().bucket(bucket).delete(Delete.builder().objects(identifiers).build()).build();
				s3Client.deleteObjects(deleteObjectsRequest);
				identifiers.clear();
			}

		}
		
		if(identifiers.size() > 0)
		{
			DeleteObjectsRequest deleteObjectsRequest = DeleteObjectsRequest.builder().bucket(bucket).delete(Delete.builder().objects(identifiers).build()).build();
			s3Client.deleteObjects(deleteObjectsRequest);

		}
		
		DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucket).build();
		s3Client.deleteBucket(deleteBucketRequest);
	}
	
	public void deleteAllBuckets() {
		List<String> buckets = this.listBuckets();
		buckets.parallelStream().forEach(x->this.deleteBucket(x));
	}
	
	public DataObject addBucket(DataObject dataObject) {
		dataObject.setName(dataObject.getName() + System.currentTimeMillis());
		CreateBucketRequest createBucketRequest = CreateBucketRequest
		        .builder()
		        .bucket(dataObject.getName()).build();
		        
		s3Client.createBucket(createBucketRequest);
		
		return dataObject;		
		
	}
	
	
	public List<String> listBuckets(){
		List<String> names = new ArrayList<>();
		ListBucketsRequest listBucketsRequest = ListBucketsRequest.builder().build();
		ListBucketsResponse listBucketsResponse = s3Client.listBuckets(listBucketsRequest);
		listBucketsResponse.buckets().stream().forEach(x -> names.add(x.name()));
		return names;
	}
	
	

}
