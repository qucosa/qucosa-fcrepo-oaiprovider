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
    private final String m_set;
    private final Date m_until;

    public RecordListProvider(RecordCache cache,
                              int incompleteListSize,
                              boolean identifiers,
                              Date from,
                              Date until,
                              String prefix,
                              String set) {
        m_cache = cache;
        m_incompleteListSize = incompleteListSize;
        m_identifiers = identifiers;
        m_from = from;
        m_until = until;
        m_prefix = prefix;
        m_set = set;
    }

    public CloseableIterator<CachedContent> getList() throws
            ServerException {
        CloseableIterator<CachedContent> iter = m_cache.getRecordsContent(m_from,
                m_until,
                m_prefix,
                m_set,
                m_identifiers);
        if (iter.hasNext()) return iter;
        // else figure out why and throw the right exception
        if (!m_cache.formatExists(m_prefix)) {
            throw new CannotDisseminateFormatException(Responder.ERR_NO_SUCH_FORMAT);
        }
        closeSetinfoIfNotNull(m_set);
        throw new NoRecordsMatchException(Responder.ERR_NO_RECORDS_MATCH);
    }

    private void closeSetinfoIfNotNull(String mSet) {
        if (mSet != null) {
            CloseableIterator<SetInfo> sic = m_cache.getSetInfoContent();
            boolean supportsSets = sic.hasNext();
            try {
                sic.close();
            } catch (Exception ignored) {
            }
            if (!supportsSets) {
                throw new NoSetHierarchyException(Responder.ERR_NO_SET_HIERARCHY);
            }
        }
    }

    public CloseableIterator<String[]> getPathList() throws
            ServerException {
        CloseableIterator<String[]> iter = m_cache.getRecordsPaths(m_from,
                m_until,
                m_prefix,
                m_set);
        if (iter.hasNext()) return iter;
        // else figure out why and throw the right exception
        if (!m_cache.formatExists(m_prefix)) {
            throw new CannotDisseminateFormatException(Responder.ERR_NO_SUCH_FORMAT);
        }
        closeSetinfoIfNotNull(m_set);
        throw new NoRecordsMatchException(Responder.ERR_NO_RECORDS_MATCH);
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
