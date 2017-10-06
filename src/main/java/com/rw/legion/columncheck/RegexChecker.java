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

package com.rw.legion.columncheck;

import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

import java.util.regex.Pattern;

/**
 * A column checker that validates a string matches a given regex. Requires a
 * "pattern" parameter in the objective JSON.
 */

public class RegexChecker implements ColumnChecker {
    private Pattern pattern;
    
    public RegexChecker(JsonObject json) throws JsonParseException {
        if (! (json.has("regex"))) {
            throw new JsonParseException("RegexChecker requires regex!");
        }
        
        String regex = json.get("regex").getAsString();
        pattern = Pattern.compile(regex);
    }
    
    public boolean validates(String str) {
        return pattern.matcher(str).matches();
    }

    public Pattern getPattern() {
        return pattern;
    }

}
