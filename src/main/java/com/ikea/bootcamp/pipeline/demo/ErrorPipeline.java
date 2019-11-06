package com.ikea.bootcamp.pipeline.demo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;

import java.io.Serializable;

public class ErrorPipeline implements Serializable {

    private static String INPUT_TOPIC = "error_input";
    private static String OUTPUT_TOPIC = "error_output";

    static class PrintDataToLogs extends DoFn<String, String> {
        @ProcessElement
        public void processElement(@Element String line, OutputReceiver<String> out) {
            System.out.println("Abhishek " + line);
            out.output(line);
        }
    }

    static class ValuesFn extends DoFn<KafkaRecord<String, String>, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(c.element().getKV().getValue());
        }
    }

    public void recieveAndSendData(PipelineOptions options) {
        Pipeline pipeline = Pipeline.create(options);
        PCollection<String> output =
                pipeline
                        .apply(
                                KafkaIO.<String, String>read()
                                        .withBootstrapServers("localhost:9092")
                                        .withTopic(INPUT_TOPIC)
                                        .withKeyDeserializer(StringDeserializer.class)
                                        .withValueDeserializer(StringDeserializer.class)
                                        .updateConsumerProperties(
                                                ImmutableMap.of("auto.offset.reset", (Object) "earliest"))
                                        .withoutMetadata())
                        .apply(
                                "Apply Fixed window: ",
                                Window.<KV<String, String>>into(FixedWindows.of(Duration.standardSeconds(120))))
                        .apply(
                                MapElements.via(
                                        new SimpleFunction<KV<String, String>, String>() {
                                            private static final long serialVersionUID = 1L;

                                            @Override
                                            public String apply(KV<String, String> inputJSON) {
                                                return inputJSON.getValue();
                                            }
                                        }));
        output.apply(ParDo.of(new PrintDataToLogs()));
        output.apply(KafkaIO.<Void, String>write()
                .withBootstrapServers("localhost:9092")
                .withTopic(OUTPUT_TOPIC)
                .withValueSerializer(StringSerializer.class) // just need serializer for value
                .values()
        );
        pipeline.run();
    }
}
