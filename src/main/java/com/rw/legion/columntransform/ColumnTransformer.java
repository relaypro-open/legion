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

/**
 * Defines an interface for Legion "column transformers." A column transformer
 * accepts a string, transforms it in some fashion, and returns another string.
 * For example, it could accept a string containing a formatted float, round it,
 * and return a string containing a formatted integer.
 */

public interface ColumnTransformer {
    public String transform(String str);
}
