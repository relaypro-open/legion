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

/**
 * A column checker that validates a string is a valid float. The optional
 * property "floatType" in the objective JSON specifies what type of float to
 * check it against - float or double. Defaults to double.
 */

public class FloatChecker implements ColumnChecker {
    private String floatType;

    public class FloatOutOfBoundsException extends Exception {
        public FloatOutOfBoundsException(String message) {
            super(message);
        }
    }

    public FloatChecker(JsonObject json) {
        if (! (json.has("floatType"))) {
            floatType = "double";
        } else {
            floatType = json.get("floatType").getAsString();
        }
    }

    /**
     * Validate a string against its floatType. Infinite values are rejected,
     * but zero (or near zero) value are accepted.
     *
     * return  boolean
     * */
    
    public boolean validates(String str) {
        try {
            if (floatType.equals("float")) {
                Float value = Float.parseFloat(str);
                if (value.isInfinite()) {
                    throw new FloatOutOfBoundsException("Float exceeded range.");
                }
            }
            if (floatType.equals("double")) {
                Double value = Double.parseDouble(str);
                if (value.isInfinite()) {
                    throw new FloatOutOfBoundsException("Double exceeded range.");
                }
            }
            
            return true;
        } catch(NumberFormatException | FloatOutOfBoundsException e) {
            return false;
        }
    }

    public String getFloatType() {
        return floatType;
    }
}
