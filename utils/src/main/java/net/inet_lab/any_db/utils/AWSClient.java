package net.inet_lab.any_db.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.regions.Regions;

public class AWSClient {
    private final String access_key;
    private final String secret_key;
    private final AmazonS3 s3client;
    public AWSClient() {
        Map<String, String> env = System.getenv();

        if ((access_key = env.get("AWS_ACCESS_KEY_ID")) == null)
            throw new RuntimeException("AWS_ACCESS_KEY_ID is not defined");

        if ((secret_key = env.get("AWS_SECRET_ACCESS_KEY")) == null)
            throw new RuntimeException("AWS_SECRET_ACCESS_KEY is not defined");

        BasicAWSCredentials creds = new BasicAWSCredentials("access_key", "secret_key");
        s3client = AmazonS3ClientBuilder.standard()
                                        .withCredentials(new EnvironmentVariableCredentialsProvider())
                                        .withRegion(Regions.US_EAST_1)
                                        .build();
    }

    public void put(String uploadFileName, String bucketName, String keyName) {
        File file = new File(uploadFileName);
        if (keyName.charAt(keyName.length()-1) == '/')
            keyName += file.getName();

        try {
            s3client.putObject(new PutObjectRequest(
                    bucketName, keyName, file));

        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which " +
                    "means your request made it " +
                    "to Amazon S3, but was rejected with an error response" +
                    " for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which " +
                    "means the client encountered " +
                    "an internal error while trying to " +
                    "communicate with S3, " +
                    "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    }

    public String getCredentials (String driver) {
        if ("redshift".equalsIgnoreCase(driver))
            return "aws_access_key_id=" + access_key + ";aws_secret_access_key=" + secret_key;
        else
            return "aws_key_id='" + access_key + "' aws_secret_key='" + secret_key + "'";
    }

    public String getAccessKeyId() {
        return access_key;
    }

    public String getSecretAccessKey() {
        return secret_key;
    }

    public void put(InputStream uploadStream, long contentLength, String bucketName, String keyName) {
        try {
            ObjectMetadata objMeta = new ObjectMetadata ();
            objMeta.setContentLength(contentLength);
            s3client.putObject(new PutObjectRequest(bucketName, keyName, uploadStream, objMeta));

        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which " +
                    "means your request made it " +
                    "to Amazon S3, but was rejected with an error response" +
                    " for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which " +
                    "means the client encountered " +
                    "an internal error while trying to " +
                    "communicate with S3, " +
                    "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    }

    public List<S3ObjectSummary> list(String bucketName, String prefix) throws IOException {
        try {
            final ListObjectsV2Request req = new ListObjectsV2Request()
                    .withBucketName(bucketName)
                    .withPrefix(prefix);
            ListObjectsV2Result result;
            List<S3ObjectSummary> ret = new ArrayList<>();
            int iter = 0;
            do {
                iter ++;
                result = s3client.listObjectsV2(req);

                ret.addAll(result.getObjectSummaries());
                req.setContinuationToken(result.getNextContinuationToken());
                if (iter > 1000) {
                    System.out.println("Over " + iter + " iterations, exiting");
                    System.exit(1);
                }
            } while(result.isTruncated());

            return ret;

        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, " +
                    "which means your request made it " +
                    "to Amazon S3, but was rejected with an error response " +
                    "for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, " +
                    "which means the client encountered " +
                    "an internal error while trying to communicate" +
                    " with S3, " +
                    "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }

        return null;
    }

}
