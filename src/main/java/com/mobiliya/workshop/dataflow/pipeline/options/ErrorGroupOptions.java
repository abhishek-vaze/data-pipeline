package com.mobiliya.workshop.dataflow.pipeline.options;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface ErrorGroupOptions extends PipelineOptions {

    @Description("Error code to filter upon")
    @Validation.Required
    String getErrorCode();

    void setErrorCode(String value);

    @Description("Input topic")
    @Validation.Required
    String getInputTopic();

    void setInputTopic(String value);

    @Description("Output topic")
    @Validation.Required
    String getOutputTopic();

    void setOutputTopic(String value);

    @Description("Failure topic")
    @Validation.Required
    String getFailureTopic();

    void setFailureTopic(String value);


}
