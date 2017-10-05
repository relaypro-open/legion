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

package com.rw.legion.columntransform;

import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;


/**
 * A <code>ColumnTransformer</code> which takes a null value in a column and
 * replaces it with a specified string. Requires the property "replacement" in
 * the Objective JSON.
 */

public class NullReplaceTransformer implements ColumnTransformer {
    private String replacement;
    
    public NullReplaceTransformer(JsonObject json) throws JsonParseException {
        if (! (json.has("replacement"))) {
            throw new JsonParseException("NullReplaceTransformer requires " +
                    "replacement string!");
        }
        
        replacement = json.get("replacement").getAsString();
    }
    
    public String transform(String str) {
        if (str == null || str.equals("")) return replacement;
        return str;
    }
}
