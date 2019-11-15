package com.mobiliya.workshop.subprocess;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mobiliya.workshop.dataflow.pipeline.entities.Error;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

import java.io.IOException;


public class JsonTransformer extends PTransform<PCollection<String>, PCollection<Error>> {


    @Override
    public PCollection<Error> expand(PCollection<String> input) {

        return (PCollection) input.apply
                (MapElements.via(new SimpleFunction<String, Error>() {
                    public Error apply(String input) {
                        ObjectMapper objectMapper = new ObjectMapper();
                        try {
                            Error error = objectMapper.readValue(input, Error.class);
                            return error;
                        } catch (Exception ex) {
                            System.out.println("cannot parse input json");
                            return null;
                        }
                    }
                }));
    }
}
