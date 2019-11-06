package com.ikea.bootcamp.pipeline;

import com.ikea.bootcamp.subprocess.CheckErrorFn;
import com.ikea.bootcamp.model.ErrorGroupOptions;
import com.ikea.bootcamp.subprocess.PrintDataToLogs;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.*;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;

import java.io.Serializable;

public class ErrorPipeline implements Serializable {

    private static String INPUT_TOPIC = "error_input";
    private static String OUTPUT_TOPIC = "error_output";
    private static String IGNORE_TOPIC = "ignore_output";
    private static String KAFKA_SERVER = "localhost:9092";
    private static int WINDOW_INTERVAL = 120;

    private TupleTag<String> GROUPBY = new TupleTag<String>() {
    };
    private TupleTag<String> IGNORED = new TupleTag<String>() {
    };
    private TupleTag<String> UNPARSABLE_INPUT = new TupleTag<String>() {
    };

    public void recieveAndSendData(ErrorGroupOptions options) {

        Pipeline pipeline = Pipeline.create(options);
        PCollection<String> output =
                pipeline
                        .apply(
                                KafkaIO.<String, String>read()
                                        .withBootstrapServers(KAFKA_SERVER)
                                        .withTopic(INPUT_TOPIC)
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
                                        }));
        PCollectionTuple out = output.apply(ParDo.of(new CheckErrorFn(options.getErrorCode(), GROUPBY, IGNORED, UNPARSABLE_INPUT)).withOutputTags(GROUPBY, TupleTagList.of(IGNORED).and(UNPARSABLE_INPUT)));

        out.get(GROUPBY).apply(KafkaIO.<Void, String>write()
                .withBootstrapServers(KAFKA_SERVER)
                .withTopic(OUTPUT_TOPIC)
                .withValueSerializer(StringSerializer.class) // just need serializer for value
                .values()
        );

        out.get(IGNORED).apply(KafkaIO.<Void, String>write()
                .withBootstrapServers(KAFKA_SERVER)
                .withTopic(IGNORE_TOPIC)
                .withValueSerializer(StringSerializer.class) // just need serializer for value
                .values()
        );
        out.get(UNPARSABLE_INPUT).apply(ParDo.of(new PrintDataToLogs()));

        pipeline.run();
    }
}
