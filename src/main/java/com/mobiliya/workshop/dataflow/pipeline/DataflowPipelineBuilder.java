package com.mobiliya.workshop.dataflow.pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mobiliya.workshop.dataflow.pipeline.entities.Error;
import com.mobiliya.workshop.dataflow.pipeline.options.ErrorGroupOptions;
import com.mobiliya.workshop.subprocess.JsonTransformer;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.jackson.AsJsons;
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

    private TupleTag<Error> success = new TupleTag<Error>() {
    };
    private TupleTag<Error> failure = new TupleTag<Error>() {
    };

    public Pipeline createDataPipeline(String[] args) {

        ErrorGroupOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(ErrorGroupOptions.class);
        String errorCode = options.getErrorCode();
        Pipeline pipeline = Pipeline.create(options);


        pipeline
                .apply("Read from Kafka",
                        KafkaIO.<String, String>read()
                                .withBootstrapServers(KAFKA_SERVER)
                                .withTopic(options.getInputTopic())
                                .withKeyDeserializer(StringDeserializer.class)
                                .withValueDeserializer(StringDeserializer.class)
                                .updateConsumerProperties(ImmutableMap.of("auto.offset.reset", (Object) "earliest"))
                                .withoutMetadata())
                .apply(Values.<String>create())
                .apply(new JsonTransformer())
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



        /*PCollectionTuple out = output.apply(ParDo.of(new CheckErrorFn(options.getErrorCode(), success, failure)).withOutputTags(success, TupleTagList.of(failure)));

        out.get(success).apply(AsJsons.of(Error.class).withMapper(new ObjectMapper())).apply(KafkaIO.<Long, String>write()
                .withBootstrapServers(KAFKA_SERVER)
                .withTopic(options.getOutputTopic())
                .withValueSerializer(StringSerializer.class) // just need serializer for value
                .values()
        );

        out.get(failure).apply(AsJsons.of(Error.class).withMapper(new ObjectMapper())).apply(KafkaIO.<Void, String>write()
                .withBootstrapServers(KAFKA_SERVER)
                .withTopic(options.getFailureTopic())
                .withValueSerializer(StringSerializer.class) // just need serializer for value
                .values()
        );*/
        return pipeline;
    }
}
