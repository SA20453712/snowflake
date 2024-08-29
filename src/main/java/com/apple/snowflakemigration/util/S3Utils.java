package com.apple.snowflakemigration.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

@Component
public class S3Utils {

    private static Logger log = LoggerFactory.getLogger(S3Utils.class);

    private static final String BUCKET_NAME = "neptune-content-data";
    private static final String PREFIX = "neptune-export/6cdd7af292a1473ebb062af9cd79d5d8/";

    S3Client s3Client;

    @Value("${cloud.aws.credentials.accessKey}")
    private String accessKeyId;

    @Value("${cloud.aws.credentials.secretKey}")
    private String secretKey;

    @Value("${cloud.aws.credentials.sessionToken}")
    private String sessionToken;

    private ObjectMapper objectMapper;

    @PostConstruct
    public void init(){
        objectMapper = new ObjectMapper();
    }

   public void getS3Client(){

        System.out.println("accessKeyId: "+accessKeyId);
        System.out.println("secretKey: "+secretKey);
        System.out.println("sessionToken: "+sessionToken);

       // Create AWS session credentialsTest
       AwsSessionCredentials sessionCredentials = AwsSessionCredentials.create(
               accessKeyId,
               secretKey,
               sessionToken
       );

       // Create an S3 client
        s3Client = S3Client.builder()
               .region(Region.US_EAST_1) // Replace with your region
               .credentialsProvider(StaticCredentialsProvider.create(sessionCredentials))
               .build();


   }
    public List<S3Object> listFiles(String folderName) {
        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(BUCKET_NAME)
                .prefix(PREFIX+folderName + "/")
                .build();

        ListObjectsV2Response response = s3Client.listObjectsV2(request);
        return response.contents();
    }

    public List<JsonNode> getFileContent(String key) {

        List<JsonNode> jsonList = new ArrayList<>();
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(BUCKET_NAME)
                .key(key)
                .build();

        try (InputStream inputStream = s3Client.getObject(request);
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            // Read the entire content as a single string
            StringBuilder contentBuilder = new StringBuilder();
            contentBuilder.append("[");
            String line;
            while ((line = reader.readLine()) != null) {
                line+= ",";
                contentBuilder.append(line);
            }
            contentBuilder.deleteCharAt(contentBuilder.length()-1);
            contentBuilder.append("]");

            // Parse the content into a list of JSON objects
           // ObjectMapper objectMapper = new ObjectMapper();
            JsonNode rootNode = objectMapper.readTree(contentBuilder.toString());
            if (rootNode.isArray()) {
                for (JsonNode node : rootNode) {
                    jsonList.add(node);
                }
            } else {
                throw new IllegalArgumentException("Expected JSON array but found a different structure.");
            }
        } catch (Exception e) {
            log.error("S3Utils.getFileContent(): Error while getting content for key ={}",key,e);
        }

        return jsonList;
    }
}
