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

package proai.service;

import proai.Writable;

/**
 * The data part of an OAI response.
 * <p/>
 * This may be complete in itself, as in the case of "Identify",
 * or it may be one part in a series of "incomplete list" responses.
 */
public interface ResponseData extends Writable {

    /**
     * Get the resumption token for the next part if this is one
     * in a series of incomplete list responses (and not the last part).
     */
    String getResumptionToken();

}
