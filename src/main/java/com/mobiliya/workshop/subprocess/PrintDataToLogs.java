package com.mobiliya.workshop.subprocess;

import org.apache.beam.sdk.transforms.DoFn;

public class PrintDataToLogs extends DoFn<String, String> {
    @ProcessElement
    public void processElement(@Element String line, OutputReceiver<String> out) {
        System.err.println(line);
        out.output(line);
    }
}
