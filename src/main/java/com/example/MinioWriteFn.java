package com.example;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.Setup;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.core.sync.RequestBody;

import java.net.URI;

public class MinioWriteFn extends DoFn<String, Void> {

    private transient S3Client s3;

    @Setup
    public void setup() {

        s3 = S3Client.builder()
                .endpointOverride(URI.create("http://localhost:9000"))
                .credentialsProvider(
                        StaticCredentialsProvider.create(
                                AwsBasicCredentials.create("minioadmin", "minioadmin")
                        )
                )
                .region(Region.US_EAST_1)

                // 🔥 REQUIRED FOR MINIO
                .serviceConfiguration(
                        S3Configuration.builder()
                                .pathStyleAccessEnabled(true)
                                .build()
                )
                .build();
    }

    @ProcessElement
    public void processElement(ProcessContext c) {

        String value = c.element();
        String key = "msg-" + System.currentTimeMillis() + ".txt";

        s3.putObject(
                PutObjectRequest.builder()
                        .bucket("cust-details")
                        .key(key)
                        .build(),
                RequestBody.fromString(value)
        );

        System.out.println("📦 Written to MinIO: " + key);
    }
}