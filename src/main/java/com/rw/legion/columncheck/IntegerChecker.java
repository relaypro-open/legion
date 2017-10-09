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
 * A column checker that validates a string is a valid integer. The optional
 * property "intType" in the objective JSON specifies what type of integer to
 * check it against - short, int, or long. Defaults to int.
 */

public class IntegerChecker implements ColumnChecker {
    private int safeLength;
    private String intType;
    
    public IntegerChecker(JsonObject json) {
        if (! (json.has("intType"))) {
            intType = "int";
            safeLength = 9;
        } else {
            intType = json.get("intType").getAsString();
            
            if (intType.equals("short")) safeLength = 4;
            else if (intType.equals("int")) safeLength = 9;
            else if (intType.equals("long")) safeLength = 15;
        }
    }
    
    public boolean validates(String str) {
        // h/t Jonas Klemming on StackOverflow
        
        if (str == null) {
            return false;
        }
        
        int length = str.length();
        
        if (length == 0) {
            return false;
        } else if (length > safeLength) {
            // String is long enough that we need to worry about overflows.
            try {
                if (intType.equals("short")) Short.parseShort(str);
                if (intType.equals("int")) Integer.parseInt(str);
                if (intType.equals("long")) Long.parseLong(str);
                
                return true;
            } catch(NumberFormatException e) {
                return false;
            }
        }
        
        int i = 0;
        
        if (str.charAt(0) == '-') {
            if (length == 1) {
                return false;
            }
            
            i = 1;
        }
        
        for (; i < length; i++) {
            char c = str.charAt(i);
            
            if (c < '0' || c > '9') {
                return false;
            }
        }
        
        return true;
    }

    public String getIntType() {
        return intType;
    }

    public int getSafeLength() {
        return safeLength;
    }
}
