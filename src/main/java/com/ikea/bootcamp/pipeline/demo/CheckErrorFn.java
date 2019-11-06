package com.ikea.bootcamp.pipeline.demo;

import com.google.gson.Gson;
import org.apache.beam.sdk.transforms.DoFn;

import java.io.Serializable;
import java.util.Objects;

public class CheckErrorFn extends DoFn<String, String> implements Serializable {

    private String errorCode;

    public CheckErrorFn(String errorCode) {
        this.errorCode = errorCode;
    }

    @ProcessElement
    public void process(@Element String line, ProcessContext c) {
        Gson gson = new Gson();

        Error error = null;
        try {
            error = gson.fromJson(line, Error.class);
        } catch (Exception ex) {
            System.out.println("Handled json parsing error");
        }
        if (Objects.isNull(error)) {
            System.out.println("Incorrect Json");
        } else if (error.getErrorCode().equalsIgnoreCase(errorCode)) {
            System.out.println("Error accepted");
            c.output(gson.toJson(error));
        } else {
            System.out.println("Ignored error");
        }
    }
}
