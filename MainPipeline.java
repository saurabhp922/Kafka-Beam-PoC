package com.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class MainPipeline {

    public static void main(String[] args) {

        Pipeline p = Pipeline.create(
                PipelineOptionsFactory.fromArgs(args).create()
        );

        PCollection<String> messages =
                p.apply("Read from Kafka",
                                KafkaIO.<String, String>read()
                                        .withBootstrapServers("localhost:9092")
                                        .withTopic("input-topic")
                                        .withKeyDeserializer(StringDeserializer.class)
                                        .withValueDeserializer(StringDeserializer.class)
                                        .withoutMetadata()
                        )
                        .apply("Extract Value",
                                MapElements.into(TypeDescriptors.strings())
                                        .via(record -> record.getValue())
                        );

        // Print
        messages.apply("Print",
                MapElements.into(TypeDescriptors.strings())
                        .via(msg -> {
                            System.out.println("GOT: " + msg);
                            return msg;
                        })
        );

        // Kafka sink
        messages.apply("Write to Kafka",
                KafkaIO.<Void, String>write()
                        .withBootstrapServers("localhost:9092")
                        .withTopic("output-topic")
                        .withValueSerializer(StringSerializer.class)
                        .values()
        );

        // MinIO sink
        messages.apply("Write to MinIO",
                ParDo.of(new MinioWriteFn())
        );

        p.run().waitUntilFinish();
    }
}