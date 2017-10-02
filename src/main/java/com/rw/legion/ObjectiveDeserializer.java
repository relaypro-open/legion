/*
 * Copyright (C) 2017 Republic Wireless
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rw.legion;

import com.google.gson.*;
import com.rw.legion.columncheck.ColumnChecker;
import com.rw.legion.columncheck.StringChecker;
import com.rw.legion.columntransform.ColumnTransformer;

import java.lang.reflect.Type;

/**
 * Handles deserializing Legion Objectives from JSON. Most of the work is
 * handled by Gson, but a custom deserializer is used for
 * <code>OutputColumn</code> in order to flexibly instantiate the proper
 * column check and column transformation classes.
 */

public class ObjectiveDeserializer {
    
    /**
     * Deserialize a JSON string into a LegionObjective.
     * 
     * @param json  The JSON string to be deserialized.
     * 
     * @return  A deserialized LegionObjective.
     */    
    public static LegionObjective deserialize(String json) {
        GsonBuilder builder = new GsonBuilder();
        builder.registerTypeAdapter(OutputColumn.class,
                new ColumnDeserializer());
        
        LegionObjective objective = builder.create().
                fromJson(json, LegionObjective.class);
        
        return objective;
    }
    
    /**
     * Custom Gson JsonDeserializer for <code>OutputColumn</code>. Allows
     * for setting up the proper <code>ColumnTransformer</code> and
     * <code>ColumnChecker</code> for each column.
     */
    
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
