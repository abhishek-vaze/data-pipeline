package com.mobiliya.workshop.dataflow.pipeline.options;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface ErrorGroupOptions extends PipelineOptions {

    @Description("Error code to group upon")
    @Default.String("500")
    @Validation.Required
    String getErrorCode();

    void setErrorCode(String value);


}
