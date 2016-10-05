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

package proai.util;

import java.util.List;

/**
 * Interface for a converter of TableSpec objects to
 * RDBMS-specific DDL code.</p>
 * <p/>
 * <p>Implementations of this class must be thread-safe.</p>
 * <p>Implementations must also have a public no-arg constructor.</p>
 */
public interface DDLConverter {

    List<String> getDDL(TableSpec tableSpec);

    String getDropDDL(String command);

}

