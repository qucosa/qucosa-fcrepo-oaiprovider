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
import proai.error.ServerException;

import java.io.PrintWriter;

public class ResponseDataImpl implements ResponseData {

    private final String m_resumptionToken;
    private final Writable m_writable;

    public ResponseDataImpl(Writable writable) {
        m_writable = writable;
        m_resumptionToken = null;
    }

    public ResponseDataImpl(Writable writable, String resumptionToken) {
        m_writable = writable;
        m_resumptionToken = resumptionToken;
    }

    public void write(PrintWriter out) throws ServerException {
        m_writable.write(out);
    }

    public String getResumptionToken() {
        return m_resumptionToken;
    }

}
