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

package proai.cache;

import proai.CloseableIterator;
import proai.error.ServerException;

public class CachedRecordContentIterator implements CloseableIterator<CachedContent> {

    private final CloseableIterator<String[]> m_arrays;
    private final boolean m_headersOnly;
    private final RCDisk m_rcDisk;
    private boolean m_closed;

    CachedRecordContentIterator(CloseableIterator<String[]> paths,
                                RCDisk rcDisk,
                                boolean headersOnly) {
        m_arrays = paths;
        m_rcDisk = rcDisk;
        m_headersOnly = headersOnly;

        m_closed = false;
    }

    public void finalize() {
        close();
    }

    public boolean hasNext() throws ServerException {
        return m_arrays.hasNext();
    }

    public CachedContent next() throws ServerException {
        if (!hasNext()) return null;
        try {
            String[] array = m_arrays.next();
            return m_rcDisk.getContent(array[0], m_headersOnly);
        } catch (Exception e) {
            close();
            throw new ServerException("Could not get next record content from iterator", e);
        }
    }

    public void close() {
        if (!m_closed) {
            m_closed = true;
            m_arrays.close();
        }
    }


    public void remove() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("CachedRecordContentIterator does not support remove().");
    }

}
