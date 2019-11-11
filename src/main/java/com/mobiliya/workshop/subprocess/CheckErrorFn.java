package com.mobiliya.workshop.subprocess;

import com.google.gson.Gson;
import com.mobiliya.workshop.dataflow.pipeline.entities.Error;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;

import java.io.Serializable;

public class CheckErrorFn extends DoFn<String, String> implements Serializable {

    private String errorCode;
    private TupleTag<String> groupBy;
    private TupleTag<String> ignored;
    private TupleTag<String> unparsableInput;

    public CheckErrorFn(String errorCode, TupleTag<String> groupBy, TupleTag<String> ignored, TupleTag<String> unparsableInput) {
        this.groupBy = groupBy;
        this.ignored = ignored;
        this.unparsableInput = unparsableInput;
        this.errorCode = errorCode;
    }

    @ProcessElement
    public void process(@Element String line, MultiOutputReceiver out) {
        Gson gson = new Gson();
        Error error = null;
        try {
            error = gson.fromJson(line, Error.class);
        } catch (Exception ex) {
            out.get(unparsableInput).output(line);
            return;
        }

        if (error.getErrorCode().equalsIgnoreCase(errorCode))
            out.get(groupBy).output(gson.toJson(error));
        else
            out.get(ignored).output(gson.toJson(error));

    }
}
