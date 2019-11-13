package com.mobiliya.workshop.subprocess;

import com.mobiliya.workshop.dataflow.pipeline.entities.Error;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;

public class CheckErrorFn extends DoFn<Error, Error> implements Serializable {

    private String errorCode;
    private TupleTag<Error> success;
    private TupleTag<Error> failure;

    public CheckErrorFn(String errorCode, TupleTag<Error> success, TupleTag<Error> failure) {
        this.success = success;
        this.failure = failure;
        this.errorCode = errorCode;
    }

    @ProcessElement
    public void process(@Element Error error, MultiOutputReceiver out) throws Exception {

        if (error.getErrorCode().equalsIgnoreCase(errorCode))
            out.get(success).output(error);
        else
            out.get(failure).output(error);

    }
}
