package net.inet_lab.any_db.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

public class AWSClient {
    private final String access_key;
    private final String secret_key;
    private final S3Client s3client;

    public AWSClient() {
        Map<String, String> env = System.getenv();

        if ((access_key = env.get("AWS_ACCESS_KEY_ID")) == null)
            throw new RuntimeException("AWS_ACCESS_KEY_ID is not defined");

        if ((secret_key = env.get("AWS_SECRET_ACCESS_KEY")) == null)
            throw new RuntimeException("AWS_SECRET_ACCESS_KEY is not defined");

        s3client = S3Client.builder()
                           .credentialsProvider(EnvironmentVariableCredentialsProvider.create())
                           .region(Region.US_EAST_1)
                           .build();
    }

    public void put(String uploadFileName, String bucketName, String keyName) {
        File file = new File(uploadFileName);
        if (keyName.charAt(keyName.length()-1) == '/')
            keyName += file.getName();

        try {
            s3client.putObject(putRequest(bucketName, keyName), RequestBody.fromFile(file));
        } catch (AwsServiceException ase) {
            reportServiceException(ase);
        } catch (SdkClientException ace) {
            reportClientException(ace);
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
            s3client.putObject(putRequest(bucketName, keyName), RequestBody.fromInputStream(uploadStream, contentLength));
        } catch (AwsServiceException ase) {
            reportServiceException(ase);
        } catch (SdkClientException ace) {
            reportClientException(ace);
        }
    }

    public List<S3Object> list(String bucketName, String prefix) throws IOException {
        try {
            ListObjectsV2Request.Builder req = ListObjectsV2Request.builder()
                    .bucket(bucketName)
                    .prefix(prefix);
            ListObjectsV2Response result;
            List<S3Object> ret = new ArrayList<>();
            int iter = 0;
            do {
                iter ++;
                result = s3client.listObjectsV2(req.build());

                ret.addAll(result.contents());
                req.continuationToken(result.nextContinuationToken());
                if (iter > 1000) {
                    System.out.println("Over " + iter + " iterations, exiting");
                    System.exit(1);
                }
            } while(Boolean.TRUE.equals(result.isTruncated()));

            return ret;

        } catch (AwsServiceException ase) {
            reportServiceException(ase);
        } catch (SdkClientException ace) {
            reportClientException(ace);
        }

        return null;
    }

    private static PutObjectRequest putRequest(String bucketName, String keyName) {
        return PutObjectRequest.builder()
                .bucket(bucketName)
                .key(keyName)
                .build();
    }

    private static void reportServiceException(AwsServiceException ase) {
        System.out.println("Caught an AwsServiceException, which " +
                "means your request made it " +
                "to Amazon S3, but was rejected with an error response" +
                " for some reason.");
        System.out.println("Error Message:    " + ase.getMessage());
        System.out.println("HTTP Status Code: " + ase.statusCode());
        if (ase.awsErrorDetails() != null)
            System.out.println("AWS Error Code:   " + ase.awsErrorDetails().errorCode());
        System.out.println("Request ID:       " + ase.requestId());
    }

    private static void reportClientException(SdkClientException ace) {
        System.out.println("Caught an SdkClientException, which " +
                "means the client encountered " +
                "an internal error while trying to " +
                "communicate with S3, " +
                "such as not being able to access the network.");
        System.out.println("Error Message: " + ace.getMessage());
    }
}
