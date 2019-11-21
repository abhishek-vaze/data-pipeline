package com.mobiliya.workshop.dataflow.pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mobiliya.workshop.dataflow.pipeline.entities.Error;
import com.mobiliya.workshop.dataflow.pipeline.options.ErrorGroupOptions;
import com.mobiliya.workshop.subprocess.JsonSchemaValidator;
import com.mobiliya.workshop.subprocess.JsonValidationPredicate;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.jackson.AsJsons;
import org.apache.beam.sdk.extensions.jackson.ParseJsons;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.*;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;

import java.io.Serializable;

public class DataflowPipelineBuilder implements Serializable {

    private static String KAFKA_SERVER = "localhost:9092";
    private static int WINDOW_INTERVAL = 120;

    private TupleTag<String> success = new TupleTag<String>() {
    };
    private TupleTag<String> failure = new TupleTag<String>() {
    };

    public Pipeline createDataPipeline(String[] args) {

        ErrorGroupOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(ErrorGroupOptions.class);
        String errorCode = options.getErrorCode();
        Pipeline pipeline = Pipeline.create(options);
        JsonValidationPredicate validator = new JsonValidationPredicate();


        PCollectionTuple out = pipeline
                .apply("Read from Kafka",
                        KafkaIO.<String, String>read()
                                .withBootstrapServers(KAFKA_SERVER)
                                .withTopic(options.getInputTopic())
                                .withKeyDeserializer(StringDeserializer.class)
                                .withValueDeserializer(StringDeserializer.class)
                                .withConsumerConfigUpdates(ImmutableMap.of("auto.offset.reset", (Object) "earliest"))
                                .withoutMetadata())
                .apply(Values.<String>create())
                .apply("Schema validation",
                        ParDo.of(new JsonSchemaValidator(success, failure, validator)).withOutputTags(success, TupleTagList.of(failure)));

        out.get(success)
                .apply("Deserialize from JSON ",ParseJsons.of(Error.class))
                .apply("Filter by Error Code",
                        Filter.by(input -> {
                            return input.getErrorCode().equalsIgnoreCase(errorCode);
                        }))
                .apply(
                        "Apply Fixed window: ",
                        Window.<Error>into(FixedWindows.of(Duration.standardSeconds(WINDOW_INTERVAL))))
                .apply("Serialize to JSON",
                        AsJsons.of(Error.class).withMapper(new ObjectMapper()))
                .apply("Write to Kafka",
                        KafkaIO.<Void, String>write()
                                .withBootstrapServers(KAFKA_SERVER)
                                .withTopic(options.getOutputTopic())
                                .withValueSerializer(StringSerializer.class) // just need serializer for value
                                .values());

        out.get(failure).apply(KafkaIO.<Void, String>write()
                .withBootstrapServers(KAFKA_SERVER)
                .withTopic(options.getFailureTopic())
                .withValueSerializer(StringSerializer.class) // just need serializer for value
                .values()
        );
        return pipeline;
    }
}
