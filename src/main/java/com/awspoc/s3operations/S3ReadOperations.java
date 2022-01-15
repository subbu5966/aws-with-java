package com.awspoc.s3operations;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

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

/**
 * 
 * @author subbu
 *
 */

public class S3ReadOperations {

	private final String ROOT_BUCKET_NAME = "<bucketname>";

	/* s3 client with credentials */
	private final AWSCredentials credentials = new BasicAWSCredentials("<access_key_id>", "<secret_access_key>");
	private final AmazonS3 s3ClientWithCreds = AmazonS3ClientBuilder.standard()
			.withCredentials(new AWSStaticCredentialsProvider(credentials)).withRegion(Regions.US_EAST_1).build();
	/* s3 client with credentials */

	final AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();

	private void getAllFromSubFolder() {
		try {
			List<S3ObjectSummary> listOfObjectSummaries = getAllObjectFromSubFolder("<Root_Folder>/<subfolder>/");
			System.out.println("list of s3 objects " + listOfObjectSummaries.size());
			for (S3ObjectSummary s3ObjectSummary : listOfObjectSummaries) {
				String s3Key = s3ObjectSummary.getKey();
				System.out.println("key " + s3Key);
				S3Object object = s3ClientWithCreds.getObject(ROOT_BUCKET_NAME, s3Key);
				S3ObjectInputStream inputStream = object.getObjectContent();
				/* use below input stream conversion for java 9 and above */
				String jsonFile = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
				/* ------- */
				/* use below input stream conversion for java 8 and above */
				String java8JsonFile = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))
						.lines().collect(Collectors.joining("\n"));
				/* ------ */
				System.out.println(jsonFile);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void getAllFromS3() {
		try {
			List<S3ObjectSummary> listOfObjectSummaries = getAllObjects();
			for (S3ObjectSummary s3ObjectSummary : listOfObjectSummaries) {
				String s3Key = s3ObjectSummary.getKey();
				System.out.println("key " + s3Key);
				S3Object object = s3ClientWithCreds.getObject(ROOT_BUCKET_NAME, s3Key);
				S3ObjectInputStream inputStream = object.getObjectContent();
				/* use below input stream conversion for java 9 and above */
				String jsonFile = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
				/* ------- */
				/* use below input stream conversion for java 8 and above */
				String java8JsonFile = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))
						.lines().collect(Collectors.joining("\n"));
				/* ------ */
				System.out.println(jsonFile);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * virtually s3 object can have infinite number of objects, but it's not
	 * possible to load all at once into collection. The max limit to is 1000
	 * object, means if we use getObjectSummaries() method it will return only 1000
	 * objects. In order to collect all the object in to list we need to use
	 * getNextContinuationToken(), this will act like pagination and load object
	 * page by page. One page container 1k objects.
	 * 
	 * If we want all object from sub folders we need to use prepare request object
	 * with bucketname and prefix.
	 * 
	 * Prefix Examples
	 * 
	 * 1) prefix for rootbucket/<rootfolder>/<filename> is <rootfolder>/<filename>/
	 * 2) prefix for rootbucket/<rootfolder>/<subfolder1>/<filename> is
	 * <rootfolder>/<subfolder1>/ 3) prefix for
	 * rootbucket/<rootfolder>/<subfolder1>/<subfolder2>/<filename> is
	 * <rootfolder>/<subfolder1>/<subfolder2>/
	 * 
	 * @param s3client
	 * @return
	 * @throws IOException
	 */
	private List<S3ObjectSummary> getAllObjectFromSubFolder(String prefix) throws IOException {
		ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(ROOT_BUCKET_NAME).withPrefix(prefix);
		ListObjectsV2Result result;
		List<S3ObjectSummary> listOfObjectSummaries = new ArrayList();
		do {
			result = s3ClientWithCreds.listObjectsV2(req);
			listOfObjectSummaries.addAll(result.getObjectSummaries());
			String token = result.getNextContinuationToken();
			System.out.println("Next Continuation Token: " + token);
			req.setContinuationToken(token);
		} while (result.isTruncated());
		return listOfObjectSummaries;
	}

	private List<S3ObjectSummary> getAllObjects() {
		ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(ROOT_BUCKET_NAME);
		ListObjectsV2Result result;
		List<S3ObjectSummary> listOfObjectSummaries = new ArrayList();
		try {
			do {
				result = s3ClientWithCreds.listObjectsV2(req);
				listOfObjectSummaries.addAll(result.getObjectSummaries());
				String token = result.getNextContinuationToken();
				System.out.println("Next Continuation Token: " + token);
				req.setContinuationToken(token);
			} while (result.isTruncated());
		} catch (Exception e) {
			e.printStackTrace();
		}
		return listOfObjectSummaries;
	}

	/**
	 * If s3 contains less than 1000 objects, no need of pagination we can directly
	 * use getObjectSummaries()
	 * 
	 * @param s3client
	 * @throws IOException
	 */

	private void get1kFromSubFolder(String prefix) throws IOException {
		ObjectListing objectsList = getListObjectRequest(prefix);
		List<S3ObjectSummary> objectSummariesList = objectsList.getObjectSummaries();
		for (S3ObjectSummary s3ObjectSummary : objectSummariesList) {
			String s3Key = s3ObjectSummary.getKey();
			System.out.println("key " + s3Key);
			S3Object object = s3ClientWithCreds.getObject(ROOT_BUCKET_NAME, s3Key);
			S3ObjectInputStream inputStream = object.getObjectContent();
			/* use below input stream conversion for java 9 and above */
			String jsonFile = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
			/* ------- */
			/* use below input stream conversion for java 8 and above */
			String java8JsonFile = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))
					.lines().collect(Collectors.joining("\n"));
			/* ------ */
			System.out.println(jsonFile);
		}
	}

	private ObjectListing getListObjectRequest(String prefix) {
		ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(ROOT_BUCKET_NAME)
				.withPrefix(prefix);
		ObjectListing objectsList = s3Client.listObjects(listObjectsRequest);
		return objectsList;
	}

	private void getSingleObject(String key) {
		try {
			S3Object object = s3ClientWithCreds.getObject(ROOT_BUCKET_NAME, key);
			S3ObjectInputStream inputStream = object.getObjectContent();
			/* use below input stream conversion for java 9 and above */
			String jsonFile = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
			/* ------- */
			/* use below input stream conversion for java 8 and above */
			String java8JsonFile = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))
					.lines().collect(Collectors.joining("\n"));
			/* ------ */
			System.out.println(jsonFile);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
