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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import proai.error.BadResumptionTokenException;
import proai.error.ServerException;

import java.io.File;
import java.util.*;

class SessionManager extends Thread {

    private static final String PROP_BASEDIR = "proai.sessionBaseDir";
    private static final String PROP_SECONDSBETWEENREQUESTS = "proai.secondsBetweenRequests";
    private static final String ERR_RESUMPTION_SYNTAX_SLASH = "bad syntax in resumption token: must contain exactly one slash";
    private static final String ERR_RESUMPTION_SYNTAX_INTEGER = "bad syntax in resumption token: expected an integer after the slash";
    private static final String ERR_RESUMPTION_SESSION = "bad session id or session expired";
    private static final Logger logger = LoggerFactory.getLogger(SessionManager.class);
    private final Map<String, Session> m_sessions = new HashMap<>();
    private File m_baseDir;
    private int m_secondsBetweenRequests;
    private boolean m_threadFinished;
    private boolean m_threadNeedsToFinish;

    public SessionManager(Properties props) throws ServerException {
        String dir = props.getProperty(PROP_BASEDIR);
        if (dir == null) throw new ServerException("Required property missing: " + PROP_BASEDIR);
        String sec = props.getProperty(PROP_SECONDSBETWEENREQUESTS);
        if (sec == null) throw new ServerException("Required property missing: " + PROP_SECONDSBETWEENREQUESTS);
        int secondsBetweenRequests;
        try {
            secondsBetweenRequests = Integer.parseInt(sec);
        } catch (Exception e) {
            throw new ServerException("Required property must an integer: " + PROP_SECONDSBETWEENREQUESTS);
        }
        init(new File(dir), secondsBetweenRequests);
    }

    private void init(File baseDir, int secondsBetweenRequests) throws ServerException {
        m_baseDir = baseDir;
        m_baseDir.mkdirs();
        File[] dirs = m_baseDir.listFiles();
        if (dirs == null) throw new ServerException("Unable to create session directory: " + m_baseDir.getPath());
        if (dirs.length > 0) {
            logger.info("Cleaning up " + dirs.length + " sessions from last run...");
            try {
                Thread.sleep(4000);
            } catch (Exception ignored) {
            }
            for (File dir : dirs) {
                if (dir.isDirectory()) {
                    File[] files = dir.listFiles();
                    for (File file : files) {
                        file.delete();
                    }
                }
                dir.delete();
            }
        }

        m_secondsBetweenRequests = secondsBetweenRequests;
        setName("Session-Reaper");
        start();
    }

    //////////////////////////////////////////////////////////////////////////

    /**
     * Session timeout reaper thread.
     */
    public void run() {
        // each session has an associated thread which will stop as soon as it 
        // finishes iterating.  The purpose of *this* thread is to 
        // 1) removed the session from the map and 
        // 2) make sure any files created by sessions are cleaned up

        while (!m_threadNeedsToFinish) {
            cleanupSessions(false);
            int c = 0;
            while (c < 20 && !m_threadNeedsToFinish) {
                c++;
                try {
                    Thread.sleep(250);
                } catch (Exception ignored) {
                }
            }

        }
        m_threadFinished = true;
    }

    /**
     * If force is false, clean up expired sessions.
     * If force is true, clean up all sessions.
     */
    private void cleanupSessions(boolean force) {
        List<String> toCleanKeys = new ArrayList<>();
        List<Session> toCleanSessions = new ArrayList<>();
        synchronized (m_sessions) {
            for (String key : m_sessions.keySet()) {
                Session sess = m_sessions.get(key);
                if (force || sess.hasExpired()) {
                    toCleanKeys.add(key);
                    toCleanSessions.add(sess);
                }
            }
            if (toCleanKeys.size() > 0) {
                String dueTo;
                if (force) {
                    dueTo = "shutdown)";
                } else {
                    dueTo = "expired)";
                }
                logger.info("Cleaning up " + toCleanKeys.size() + " sessions (" + dueTo);
                for (int i = 0; i < toCleanKeys.size(); i++) {
                    toCleanSessions.get(i).clean();
                    m_sessions.remove(toCleanKeys.get(i));
                }
            }
        }
    }

    //////////////////////////////////////////////////////////////////////////

    /**
     * Add a session to the map of tracked sessions.
     * This is called by any session that has multiple responses.
     */
    void addSession(String key, Session session) {
        if (m_threadNeedsToFinish) {
            session.clean();
        } else {
            synchronized (m_sessions) {
                m_sessions.put(key, session);
            }
        }
    }

    //////////////////////////////////////////////////////////////////////////

    public <T> ResponseData list(ListProvider<T> provider) throws ServerException {
        Session session = new CacheSession<>(this, m_baseDir, m_secondsBetweenRequests, provider);
        return session.getResponseData(0);
    }

    /**
     * Get response data from the appropriate session and return it.
     * <p/>
     * The resumption token encodes the session id and part number.
     * The first part is sessionid/0, the second part is sessionid/1, and so on.
     */
    public ResponseData getResponseData(String resumptionToken)
            throws
            ServerException {
        String[] s = resumptionToken.split("/");
        if (s.length != 2) {
            throw new BadResumptionTokenException(ERR_RESUMPTION_SYNTAX_SLASH);
        }
        int partNum;
        try {
            partNum = Integer.parseInt(s[1]);
        } catch (Exception e) {
            throw new BadResumptionTokenException(ERR_RESUMPTION_SYNTAX_INTEGER);
        }
        Session session;
        synchronized (m_sessions) {
            session = m_sessions.get(s[0]);
        }
        if (session == null) {
            throw new BadResumptionTokenException(ERR_RESUMPTION_SESSION);
        }
        return session.getResponseData(partNum);
    }

    /////////////////////////////////////////////////////////////////////////

    public void finalize() throws Throwable {
        super.finalize();
        close();
    }

    public void close() {
        m_threadNeedsToFinish = true;
        while (!m_threadFinished) {
            try {
                Thread.sleep(250);
            } catch (Exception ignored) {
            }
        }
        cleanupSessions(true);
    }

}
