package com.rw.legion;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonParseException;
import com.google.gson.JsonObject;

import java.lang.reflect.Type;

import com.rw.legion.columncheck.ColumnChecker;
import com.rw.legion.columncheck.StringChecker;
import com.rw.legion.columntransform.ColumnTransformer;

public class ObjectiveDeserializer {
    public static LegionObjective deserialize(String json) {
        GsonBuilder builder = new GsonBuilder();
        builder.registerTypeAdapter(OutputColumn.class,
                new ColumnDeserializer());
        
        LegionObjective objective = builder.create().
                fromJson(json,  LegionObjective.class);
        
        return objective;
    }
    
    public static class ColumnDeserializer
            implements JsonDeserializer<OutputColumn> {
        
        @Override
        public OutputColumn deserialize(JsonElement json, Type typeOfT,
                JsonDeserializationContext ctx) throws JsonParseException {
            
            // Default de-serializer will cover everything but column checker.
            OutputColumn column = new Gson().fromJson(json, OutputColumn.class);
            
            JsonObject obj = json.getAsJsonObject();
            
            // Instantiate a column checker for this column
            ColumnChecker checker = null;
            
            if (obj.has("validate")) {
                JsonObject validate = obj.getAsJsonObject("validate");
                
                String checkerName = validate.get("class").getAsString();
                
                JsonObject checkerProps = new JsonObject();
                
                if (validate.has("options")) {
                    checkerProps = validate.getAsJsonObject("options");
                }
                
                try {
                    @SuppressWarnings("rawtypes")
                    Class[] argFormat = new Class[]{JsonObject.class};
                    
                    checker = (ColumnChecker) Class.forName(checkerName)
                            .getDeclaredConstructor(argFormat)
                            .newInstance(checkerProps);
                } catch (Exception e) {
                    throw new JsonParseException("Problem loading column " +
                            "check class '" + checkerName + "'");
                }
            } else {
                checker = new StringChecker(new JsonObject());
            }
            
            
            // Instantiate a column transformer for this column
            ColumnTransformer transformer = null;
            
            if (obj.has("transform")) {
                JsonObject transform = obj.getAsJsonObject("transform");
                
                String transformerName = transform.get("class").getAsString();
                
                JsonObject checkerProps = new JsonObject();
                
                if (transform.has("options")) {
                    checkerProps = transform.getAsJsonObject("options");
                }
                
                try {
                    @SuppressWarnings("rawtypes")
                    Class[] argFormat = new Class[]{JsonObject.class};
                    
                    transformer = (ColumnTransformer) Class
                            .forName(transformerName)
                            .getDeclaredConstructor(argFormat)
                            .newInstance(checkerProps);
                } catch (Exception e) {
                    throw new JsonParseException("Problem loading column " +
                            "transform class '" + transformerName + "'");
                }
            }
            
            column.initialize(checker, transformer);
            
            return column;
        }
    }
}
