package com.mobiliya.workshop.subprocess;


import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;

import java.util.function.Predicate;

public class JsonSchemaValidator extends DoFn<String, String> {

    private TupleTag<String> success;
    private TupleTag<String> failure;
    private JsonValidationPredicate validator;

    public JsonSchemaValidator(TupleTag<String> success, TupleTag<String> failure, Predicate<String> validator) {
        this.success = success;
        this.failure = failure;
        this.validator = (JsonValidationPredicate) validator;
    }

    @ProcessElement
    public void process(ProcessContext context) {
        if (validator.test(context.element()))
            context.output(success, context.element());
        else
            context.output(failure, context.element());
    }
}
