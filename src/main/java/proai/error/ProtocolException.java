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

package proai.error;

public abstract class ProtocolException extends ServerException {

    public ProtocolException(String message) {
        super(message);
    }

    public ProtocolException(String message,
                             Throwable cause) {
        super(message, cause);
    }

    /**
     * Get the error code of this exception, as defined by the OAI-PMH.
     */
    public abstract String getCode();

}
