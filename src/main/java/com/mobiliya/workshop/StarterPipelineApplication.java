package com.mobiliya.workshop;

import com.mobiliya.workshop.pipeline.DataflowPipelineBuilder;

public class StarterPipelineApplication {

    public static void main(String args[]) {

        new DataflowPipelineBuilder().createDataPipeline(args).run();
    }
}
