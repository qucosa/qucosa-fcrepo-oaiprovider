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

import proai.CloseableIterator;
import proai.SetInfo;
import proai.cache.CachedContent;
import proai.cache.RecordCache;
import proai.error.CannotDisseminateFormatException;
import proai.error.NoRecordsMatchException;
import proai.error.NoSetHierarchyException;
import proai.error.ServerException;

import java.util.Date;

public class RecordListProvider implements ListProvider<CachedContent> {

    private final RecordCache m_cache;
    private final Date m_from;
    private final boolean m_identifiers;
    private final int m_incompleteListSize;
    private final String m_prefix;
    private final String[] m_sets;
    private final Date m_until;

    public RecordListProvider(RecordCache cache,
                              int incompleteListSize,
                              boolean identifiers,
                              Date from,
                              Date until,
                              String prefix,
                              String[] set) {
        m_cache = cache;
        m_incompleteListSize = incompleteListSize;
        m_identifiers = identifiers;
        m_from = from;
        m_until = until;
        m_prefix = prefix;
        m_sets = set;
    }

    public CloseableIterator<CachedContent> getList() throws
            ServerException {
        CloseableIterator<CachedContent> iter = m_cache.getRecordsContent(m_from,
                m_until,
                m_prefix,
                m_sets,
                m_identifiers);
        if (iter.hasNext()) return iter;
        // else figure out why and throw the right exception
        if (m_cache.formatDoesNotExist(m_prefix)) {
            throw new CannotDisseminateFormatException();
        }
        if (m_sets != null) {
            closeSetinfoIfNotNull();
        }
        throw new NoRecordsMatchException();
    }

    private void closeSetinfoIfNotNull() {
        CloseableIterator<SetInfo> sic = m_cache.getSetInfoContent();
        boolean supportsSets = sic.hasNext();
        try {
            sic.close();
        } catch (Exception ignored) {
        }
        if (!supportsSets) {
            throw new NoSetHierarchyException();
        }
    }

    public CloseableIterator<String[]> getPathList() throws
            ServerException {
        CloseableIterator<String[]> iter = m_cache.getRecordsPaths(m_from,
                m_until,
                m_prefix,
                m_sets);
        if (iter.hasNext()) return iter;
        // else figure out why and throw the right exception
        if (m_cache.formatDoesNotExist(m_prefix)) {
            throw new CannotDisseminateFormatException();
        }
        if (m_sets != null) {
            closeSetinfoIfNotNull();
        }
        throw new NoRecordsMatchException();
    }

    public RecordCache getRecordCache() {
        return m_cache;
    }

    public int getIncompleteListSize() {
        return m_incompleteListSize;
    }

    public String getVerb() {
        if (m_identifiers) {
            return "ListIdentifiers";
        } else {
            return "ListRecords";
        }
    }

}
