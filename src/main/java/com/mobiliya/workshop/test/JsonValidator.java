package com.mobiliya.workshop.test;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.jackson.JsonLoader;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.github.fge.jsonschema.main.JsonSchema;
import com.github.fge.jsonschema.main.JsonSchemaFactory;

import java.io.File;

public class JsonValidator {

    public static void main(String args[]) throws Exception {
        String msg = "{\"errorCode\":\"205\",\"timestamp\":1574243582695,\"description\":\"Error in server\",\"moreInfo\":\"hello\"}";
        JsonNode goodJson = JsonLoader.fromString(msg);
        JsonNode fschema = JsonLoader.fromFile(new File("C:\\Users\\Abhishek\\IdeaProjects\\pipelineexample\\src\\main\\java\\input-message-schema.json"));
        JsonSchemaFactory factory = JsonSchemaFactory.byDefault();
        JsonSchema schema = factory.getJsonSchema(fschema);
        ProcessingReport report = schema.validate(goodJson);
        if(report.isSuccess())
        {
            System.out.println("Tagged with succes");
        }
        else
            System.out.println("Tagged with failure");
        System.out.println(report);


    }
}
