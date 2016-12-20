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
            
            String checkerName = obj.get("validation").getAsJsonObject().
                    get("class").getAsString();
            JsonObject checkerProps = obj.get("validation").getAsJsonObject().
                    get("options").getAsJsonObject();
            
            // Eventually, will be replaced with reflection...
            ColumnChecker checker = null;
            
            if (checkerName.equals("BoolChecker")) checker =
                    new BoolChecker(checkerProps);
            if (checkerName.equals("FloatChecker")) checker =
                    new FloatChecker(checkerProps);
            if (checkerName.equals("IntegerChecker")) checker =
                    new IntegerChecker(checkerProps);
            if (checkerName.equals("RegexChecker")) checker =
                    new RegexChecker(checkerProps);
            if (checkerName.equals("StringChecker")) checker =
                    new StringChecker(checkerProps);
            
            column.initialize(checker);
            
            return column;
        }
    }
}
