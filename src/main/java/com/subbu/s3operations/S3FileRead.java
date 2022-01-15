package com.subbu.s3operations;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class S3FileRead {

	private static final String SUBBUARTICLES = "subbuarticles";

	public static void main(String[] args) {
		try {
			System.out.println("credentials");
			AWSCredentials credentials = new BasicAWSCredentials("AKIAVVPCN2GCREO6DMWE",
					"EB3bGlVPQ/yKRLflbJLYIEm9YCTSKnVH+mvWJrmX");
			System.out.println("s3client");
			AmazonS3 s3client = AmazonS3ClientBuilder.standard()
					.withCredentials(new AWSStaticCredentialsProvider(credentials)).withRegion(Regions.US_EAST_1)
					.build();
			getAllObject(s3client);
			getSingleObject(s3client);
			get1kFromSubFolder(s3client);
			getAllFromSubFolder(s3client);
		} catch (Exception e) {
			e.printStackTrace();
		} catch (Error e) {
			e.printStackTrace();
		}
	}

	private static void getAllFromSubFolder(AmazonS3 s3client) throws IOException {
		ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(SUBBUARTICLES).withPrefix("Articles/Raw/");
		ListObjectsV2Result result;
		do {
			result = s3client.listObjectsV2(req);
			for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
				String s3Key = objectSummary.getKey();
				System.out.println("key " + s3Key);
				S3Object object = s3client.getObject(SUBBUARTICLES, s3Key);
				S3ObjectInputStream inputStream = object.getObjectContent();
				System.out.println(new String(inputStream.readAllBytes(), StandardCharsets.UTF_8));
				String token = result.getNextContinuationToken();
				System.out.println("Next Continuation Token: " + token);
				req.setContinuationToken(token);
			}
		} while (result.isTruncated());
	}

	private static void get1kFromSubFolder(AmazonS3 s3client) throws IOException {
		ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(SUBBUARTICLES)
				.withPrefix("Articles/Raw/");
		ObjectListing objectsList = s3client.listObjects(listObjectsRequest);
		List<S3ObjectSummary> objectSummariesList = objectsList.getObjectSummaries();
		for (S3ObjectSummary s3ObjectSummary : objectSummariesList) {
			String s3Key = s3ObjectSummary.getKey();
			System.out.println("key " + s3Key);
			S3Object object = s3client.getObject(SUBBUARTICLES, s3Key);
			S3ObjectInputStream inputStream = object.getObjectContent();
			System.out.println(new String(inputStream.readAllBytes(), StandardCharsets.UTF_8));
		}
	}

	private static void getSingleObject(AmazonS3 s3client) throws IOException {
		S3Object object = s3client.getObject(SUBBUARTICLES, "Articles/Raw/StudentSubbu.json");
		S3ObjectInputStream inputStream = object.getObjectContent();
		System.out.println(new String(inputStream.readAllBytes(), StandardCharsets.UTF_8));
	}

	private static void getAllObject(AmazonS3 s3client) {
		System.out.println("result");
		ListObjectsV2Result result = s3client.listObjectsV2(SUBBUARTICLES);
		System.out.println("objects");
		List<S3ObjectSummary> objects = result.getObjectSummaries();
		System.out.println("loop " + objects);
		for (S3ObjectSummary os : objects) {
			System.out.println("* " + os.getKey());
		}
	}

}
