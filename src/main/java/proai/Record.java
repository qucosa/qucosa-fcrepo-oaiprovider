/*
 * Copyright 2016 Saxon State and University Library Dresden (SLUB)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package proai;


public interface Record {

    String getItemID();

    String getPrefix();

    /**
     * Get a string that can be used to construct the XML of the record.
     * <p/>
     * The format of this string is defined by the implementation.
     * <p/>
     * The string will typically contain some kind of identifier or locator
     * (a file path or URL) and possibly, other attributes that may be used
     * to construct a record's XML.
     */
    String getSourceInfo();

}
