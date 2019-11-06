package com.ikea.bootcamp.pipeline.demo;

import org.apache.beam.sdk.transforms.DoFn;

public class PrintDataToLogs extends DoFn<String, String> {
    @ProcessElement
    public void processElement(@Element String line, OutputReceiver<String> out) {
        System.out.println(line);
        out.output(line);
    }
}
