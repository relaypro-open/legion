package com.rw.legion;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonParseException;
import com.google.gson.JsonObject;

import java.lang.reflect.Type;

import com.rw.legion.columncheck.*;

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
            ColumnChecker checker = null;
            
            if (obj.has("validation")) {
                JsonObject validation = obj.getAsJsonObject("validation");
                
                String checkerName = validation.get("class").getAsString();
                
                JsonObject checkerProps = new JsonObject();
                
                if (validation.has("options")) {
                    checkerProps = validation.getAsJsonObject("options");
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
            
            column.initialize(checker);
            
            return column;
        }
    }
}
