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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import proai.CloseableIterator;
import proai.error.ServerException;

import java.sql.*;
import java.text.SimpleDateFormat;

/**
 * An iterator around a database <code>ResultSet</code> that provides
 * a <code>String[]</code> for each row.
 * <p/>
 * Rows in the result set contain two values.  The first value is a
 * <code>String</code> representing a relative filesystem path.  The second
 * value is a <code>long</code> representing a date.
 * <p/>
 * The returned <code>String[]</code> for each row will have two parts:
 * The first is the relative filesystem path and the second is an
 * ISO8601-formatted date (second precision).
 */
public class StringResultIterator implements CloseableIterator<String[]> {

    private static final Logger logger = LoggerFactory.getLogger(StringResultIterator.class);

    private final Connection m_conn;
    private boolean m_closed;
    private boolean m_exhausted;
    private String[] m_nextStringArray;
    private ResultSet m_rs;
    private Statement m_stmt;

    public StringResultIterator(Connection conn,
                                Statement stmt,
                                ResultSet rs) throws ServerException {
        logger.debug("Constructing");
        m_conn = conn;
        m_stmt = stmt;
        m_rs = rs;
        m_closed = false;
        m_nextStringArray = getNext();
    }

    private String[] getNext() throws ServerException {
        if (m_exhausted) return null;
        try {
            if (m_rs.next()) {
                String[] result = new String[2];
                result[0] = m_rs.getString(1);
                Date d = new Date(m_rs.getLong(2));
                try {
                    result[1] = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").format(d);
                } catch (Exception e) { // won't happen
                    e.printStackTrace();
                }
                return result;
            } else {
                m_exhausted = true;
                close(); // since we know it was exhausted
                return null;
            }
        } catch (SQLException e) {
            close(); // since we know there was an error
            throw new ServerException("Error pre-getting next string from db", e);
        }
    }

    public boolean hasNext() {
        return m_nextStringArray != null;
    }

    public String[] next() throws ServerException {
        String[] next = m_nextStringArray;
        m_nextStringArray = getNext();
        return next;
    }

    public void remove() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("StringResultIterator does not support remove().");
    }

    public void close() {
        if (!m_closed) {
            if (m_rs != null) try {
                m_rs.close();
                m_rs = null;
            } catch (Exception ignored) {
            }
            if (m_stmt != null) try {
                m_stmt.close();
                m_stmt = null;
            } catch (Exception ignored) {
            }
            RecordCache.releaseConnection(m_conn);

            // gc and print memory stats when we're done with the
            // (potentially large) resultset.
            long startTime = System.currentTimeMillis();
            long startFreeBytes = Runtime.getRuntime().freeMemory();
            System.gc();
            long ms = System.currentTimeMillis() - startTime;
            long currentFreeBytes = Runtime.getRuntime().freeMemory();
            logger.debug(String.format(
                    "GC ran in %dms and free memory went from %d to %d bytes.",
                    ms, startFreeBytes, currentFreeBytes));

            m_closed = true;
            logger.debug("Closed.");
        }
    }

    public void finalize() {
        close();
    }

}
