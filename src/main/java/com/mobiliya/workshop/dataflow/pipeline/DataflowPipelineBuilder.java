package com.mobiliya.workshop.dataflow.pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mobiliya.workshop.dataflow.pipeline.entities.Error;
import com.mobiliya.workshop.subprocess.CheckErrorFn;
import com.mobiliya.workshop.dataflow.pipeline.options.ErrorGroupOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
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

    private TupleTag<Error> success = new TupleTag<Error>() {
    };
    private TupleTag<Error> failure = new TupleTag<Error>() {
    };

    public Pipeline createDataPipeline(String[] args) {

        ErrorGroupOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(ErrorGroupOptions.class);

        Pipeline pipeline = Pipeline.create(options);
        PCollection<Error> output =
                pipeline
                        .apply(
                                KafkaIO.<String, String>read()
                                        .withBootstrapServers(KAFKA_SERVER)
                                        .withTopic(options.getInputTopic())
                                        .withKeyDeserializer(StringDeserializer.class)
                                        .withValueDeserializer(StringDeserializer.class)
                                        .updateConsumerProperties(ImmutableMap.of("auto.offset.reset", (Object) "earliest"))
                                        .withoutMetadata())
                        .apply(
                                "Apply Fixed window: ",
                                Window.<KV<String, String>>into(FixedWindows.of(Duration.standardSeconds(WINDOW_INTERVAL))))
                        .apply(
                                MapElements.via(
                                        new SimpleFunction<KV<String, String>, String>() {
                                            private static final long serialVersionUID = 1L;

                                            @Override
                                            public String apply(KV<String, String> inputJSON) {
                                                return inputJSON.getValue();
                                            }
                                        })).apply(ParseJsons.of(Error.class)).setCoder(SerializableCoder.of(Error.class));
        PCollectionTuple out = output.apply(ParDo.of(new CheckErrorFn(options.getErrorCode(), success, failure)).withOutputTags(success, TupleTagList.of(failure)));

        out.get(success).apply(AsJsons.of(Error.class).withMapper(new ObjectMapper())).apply(KafkaIO.<Long, String>write()
                .withBootstrapServers(KAFKA_SERVER)
                .withTopic(options.getOutputTopic())
                .withValueSerializer(StringSerializer.class) // just need serializer for value
                .values()
        );

        out.get(success).apply(AsJsons.of(Error.class).withMapper(new ObjectMapper())).apply(KafkaIO.<Void, String>write()
                .withBootstrapServers(KAFKA_SERVER)
                .withTopic(options.getFailureTopic())
                .withValueSerializer(StringSerializer.class) // just need serializer for value
                .values()
        );
        return pipeline;
    }
}
