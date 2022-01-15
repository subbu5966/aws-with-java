# aws-with-java

## S3 Operations

### S3ReadOperations

com.awspoc.s3operations.S3ReadOperations contains all read related operations from s3 using java.

Two ways to build aws client. 

1) Using BasicAWSCredentials - If we have aws creds like access and secret keys
2) Using AmazonS3ClientBuilder - If we have access give at org level.

#### Methods 

1) getAllFromSubFolder - To get all the files from sub/nested folders like rootbucket/rootfolder/subfolder1/file1.json etc
2) getAllFromS3 - To get all files from root bucket.
3) getSingleObject - To get single file from bucket.
4) get1kFromSubFolder - To get 1000 object sub/nested folders like rootbucket/rootfolder/subfolder1/file1.json etc

	 virtually s3 object can have infinite number of objects, but it's not
	 possible to load all at once into collection. The max limit to is 1000
	 object, means if we use getObjectSummaries() method it will return only 1000
	 objects. In order to collect all the object in to list we need to use
	 getNextContinuationToken(), this will act like pagination and load object
	 page by page. One page container 1k objects.
	 page by page. One page container 1k objects.

