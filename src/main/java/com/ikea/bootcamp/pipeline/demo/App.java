package com.ikea.bootcamp.pipeline.demo;

import org.apache.beam.sdk.options.PipelineOptionsFactory;

public class App {

    public static void main(String args[]) {
        ErrorGroupOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(ErrorGroupOptions.class);
        ErrorPipeline errorPipeline = new ErrorPipeline();
        errorPipeline.recieveAndSendData(options);
    }
}
