package com.rw.legion;

import com.google.gson.Gson;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonParseException;
import com.google.gson.JsonObject;
import java.lang.reflect.Type;
import com.rw.legion.columncheck.*;

public class ObjectiveDeserializer {
    public static LegionObjective deserialize(String json) {
        Gson gson = new Gson();
        
        LegionObjective objective = gson.fromJson(json,  LegionObjective.class);
        
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
            
            String checkerName = obj.get("validation").getAsJsonObject().get("class").getAsString();
            JsonObject checkerProps = obj.get("validation").getAsJsonObject().get("props").getAsJsonObject();
            
            // Eventually, will be replaced with reflection...
            ColumnChecker checker = null;
            
            if (checkerName == "BoolChecker") checker = new BoolChecker(checkerProps);
            if (checkerName == "FloatChecker") checker = new FloatChecker(checkerProps);
            if (checkerName == "IntegerChecker") checker = new IntegerChecker(checkerProps);
            if (checkerName == "RegexChecker") checker = new RegexChecker(checkerProps);
            if (checkerName == "StringChecker") checker = new BoolChecker(checkerProps);
            
            column.initialize(checker);
            
            return column;
        }
    }
}
