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
import com.google.gson.JsonParser;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RegexCheckerTest {

    private JsonObject buildJson (String pattern) {
        if (pattern == null) {
            return new JsonObject();
        }

        String json = "\n" +
                "{\n" +
                "  \"regex\": \"" + pattern + "\"\n" +
                "}";

        JsonParser parser = new JsonParser();
        JsonObject obj = parser.parse(json).getAsJsonObject();

        return obj;
    }

    @Test
    void validatesValid() {
        String validRegex = "(1?)(-?)([0-9]{3})(-?)([0-9]{3})(-?)([0-9]{4})";
        String validMatch1 = "0001112233";
        String validMatch2 = "000-111-2233";
        String validMatch3 = "10001112233";
        String validMatch4 = "1-000-111-2233";
        String validNonMatch = "99988877776666";
        RegexChecker rc = new RegexChecker(buildJson(validRegex));

        assertEquals(Pattern.compile(validRegex).pattern(), rc.getPattern().pattern());
        assertEquals(true, rc.validates(validMatch1));
        assertEquals(true, rc.validates(validMatch2));
        assertEquals(true, rc.validates(validMatch3));
        assertEquals(true, rc.validates(validMatch4));
        assertEquals(false, rc.validates(validNonMatch));
    }

    @Test
    void validatesInvalid() {
        String invalidRegex = "(mismatched";
        assertThrows(PatternSyntaxException.class, () -> new RegexChecker(buildJson(invalidRegex)));
    }

    @Test
    void validatesMissing() {
        assertThrows(JsonParseException.class, () -> new RegexChecker(buildJson(null)));
    }

}
