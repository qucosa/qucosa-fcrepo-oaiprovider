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

package oaiprovider.driver;

import oaiprovider.FedoraRecord;
import oaiprovider.ResultCombiner;
import org.fcrepo.common.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import proai.driver.RemoteIterator;
import proai.error.RepositoryException;

/**
 * @author cwilper@cs.cornell.edu
 */
public class CombinerRecordIterator
        implements RemoteIterator<FedoraRecord>, Constants {

    private static final Logger logger = LoggerFactory.getLogger(CombinerRecordIterator.class);

    private final String m_aboutDissTypeURI;
    private final ResultCombiner m_combiner;
    private final String m_dissTypeURI;
    private final String m_mdPrefix;
    private String m_nextLine;

    /**
     * Initialize with combined record query results.
     */
    public CombinerRecordIterator(String mdPrefix,
                                  String dissTypeURI,
                                  String aboutDissTypeURI,
                                  ResultCombiner combiner) {
        m_mdPrefix = mdPrefix;
        m_dissTypeURI = dissTypeURI;
        m_aboutDissTypeURI = aboutDissTypeURI;
        m_combiner = combiner;
        m_nextLine = m_combiner.readLine();
    }

    public boolean hasNext() {
        return (m_nextLine != null);
    }

    public FedoraRecord next() throws RepositoryException {
        try {
            return getRecord(m_nextLine);
        } finally {
            if (m_nextLine != null) m_nextLine = m_combiner.readLine();
        }
    }

    public void close() {
        m_combiner.close();
    }

    /**
     * Ensure resources are freed up at garbage collection time.
     */
    protected void finalize() {
        close();
    }

    public void remove() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("CombinerRecordIterator does not support remove().");
    }

    /**
     * Construct a record given a line from the combiner. Expected format is:
     * "item","itemID","date","state","hasAbout"[,"setSpec1"[,"setSpec2"[,...]]]
     * For example:
     * info:fedora/nsdl:2051858,oai:nsdl.org:nsdl:10059:nsdl:2051858,2005-09-20T12:50:01,info:fedora/fedora-system:def/model#Active,true,5101,set2
     */
    private FedoraRecord getRecord(String line) throws RepositoryException {

        logger.debug("Constructing record from combined query result line: "
                + line);

        String[] parts = line.split(",");

        String itemID;
        String recordDissURI;
        String utcString;
        boolean isDeleted;
        String[] setSpecs;
        String aboutDissURI = null;

        // parse the line into values for constructing a FedoraRecord
        try {
            if (parts.length < 5) {
                throw new Exception("Expected at least 5 comma-separated values");
            }

            String pid = parts[0].substring(12); // everything after
            // info:fedora/

            itemID = parts[1];

            recordDissURI = getDissURI(pid, m_dissTypeURI);

            utcString = formatDatetime(parts[2]);

            isDeleted = !parts[3].equals(MODEL.ACTIVE.uri);

            if (parts[4].equals("true")) {
                if (m_aboutDissTypeURI != null) {
                    aboutDissURI = getDissURI(pid, m_aboutDissTypeURI);
                }
            }

            setSpecs = new String[parts.length - 5];
            System.arraycopy(parts, 5, setSpecs, 0, parts.length - 5);

        } catch (Exception e) {
            throw new RepositoryException("Error parsing combined query "
                    + "results from Fedora: " + e.getMessage() + ".  Input "
                    + "line was: " + line, e);
        }

        // if we got here, all the parameters were parsed correctly
        return new FedoraRecord(itemID,
                m_mdPrefix,
                recordDissURI,
                utcString,
                isDeleted,
                setSpecs,
                aboutDissURI);
    }

    private String getDissURI(String pid, String dissType) throws Exception {
        try {
            return "info:fedora/" + pid + dissType.substring(13);
        } catch (Throwable th) {
            throw new Exception("Dissemination type string (" + dissType + ") is too short.");
        }
    }

    /**
     * OAI requires second-level precision at most, but Fedora provides
     * millisecond precision. Fedora only uses UTC dates, so ensure UTC dates
     * are indicated with a trailing 'Z'.
     *
     * @param datetime
     * @return datetime string such as 2004-01-31T23:11:00Z
     */
    private String formatDatetime(String datetime) {
        StringBuilder sb = new StringBuilder(datetime);
        // length() - 5 b/c at most we're dealing with ".SSSZ"
        int i = sb.indexOf(".", sb.length() - 5);
        if (i != -1) {
            sb.delete(i, sb.length());
        }
        // Kowari's XSD.Datetime isn't timezone aware
        if (sb.charAt(sb.length() - 1) != 'Z') {
            sb.append('Z');
        }
        return sb.toString();
    }

}
