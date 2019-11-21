package com.mobiliya.workshop.subprocess;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.jackson.JsonLoader;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.github.fge.jsonschema.main.JsonSchema;
import com.github.fge.jsonschema.main.JsonSchemaFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.function.Predicate;

public class JsonValidationPredicate implements Predicate<String>, Serializable {
    @Override
    public boolean test(String input) {
        try {
            JsonNode inputJson = JsonLoader.fromString(input);
            JsonNode inputSchema = JsonLoader.fromFile(new File("src\\main\\resources\\input-message-schema.json"));
            JsonSchemaFactory factory = JsonSchemaFactory.byDefault();
            JsonSchema schema = factory.getJsonSchema(inputSchema);
            ProcessingReport report = schema.validate(inputJson);
            return report.isSuccess();
        } catch (FileNotFoundException fex) {
            System.err.println(fex.getMessage());
            System.exit(-1);
            return false;
        } catch (Exception ex) {
            return false;
        }
    }
}
