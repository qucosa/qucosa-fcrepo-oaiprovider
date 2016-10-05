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
import proai.cache.RecordCache;
import proai.error.NoSetHierarchyException;
import proai.error.ServerException;

public class SetListProvider implements ListProvider<SetInfo> {

    private final RecordCache m_cache;
    private final int m_incompleteListSize;

    public SetListProvider(RecordCache cache,
                           int incompleteListSize) {
        m_cache = cache;
        m_incompleteListSize = incompleteListSize;
    }

    public CloseableIterator<SetInfo> getList() throws ServerException {
        CloseableIterator<SetInfo> iter = m_cache.getSetInfoContent();
        if (iter.hasNext()) return iter;
        try {
            iter.close();
        } catch (Exception ignored) {
        }
        throw new NoSetHierarchyException();
    }

    public CloseableIterator<String[]> getPathList() throws ServerException {
        CloseableIterator<String[]> iter = m_cache.getSetInfoPaths();
        if (iter.hasNext()) return iter;
        try {
            iter.close();
        } catch (Exception ignored) {
        }
        throw new NoSetHierarchyException();
    }

    public RecordCache getRecordCache() {
        return m_cache;
    }

    public int getIncompleteListSize() {
        return m_incompleteListSize;
    }

    public String getVerb() {
        return "ListSets";
    }

}
