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
import proai.CloseableIterator;
import proai.cache.CachedContentAggregate;
import proai.error.BadResumptionTokenException;
import proai.error.ServerException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Date;

public class CacheSession<T> extends Thread
        implements Session {

    private static final Logger log = LoggerFactory.getLogger(CacheSession.class);

    private final File _baseDir;
    private final SessionManager _manager;
    private final ListProvider<T> _provider;
    private final int _secondsBetweenRequests;
    private final String _sessionKey;
    private ServerException _exception;
    private long _expirationTime;
    private int _lastGeneratedPart;
    private int _lastSentPart;
    private boolean _threadNeedsToFinish;
    private boolean _threadWorking;
    private int _threadWorkingPart;

    public CacheSession(SessionManager manager,
                        File baseDir,
                        int secondsBetweenRequests,
                        ListProvider<T> provider) {
        _manager = manager;
        _baseDir = baseDir;
        _secondsBetweenRequests = secondsBetweenRequests;
        _provider = provider;

        // make a unique key for this session
        String s = "" + this.hashCode();
        if (s.startsWith("-")) {
            _sessionKey = "Z" + s.substring(1);
        } else {
            _sessionKey = "X" + s;
        }

        _threadWorkingPart = 0;
        _lastGeneratedPart = -1;
        _lastSentPart = -1;

        _threadWorking = true;

        setName("Session-" + _sessionKey + "-Retriever");

        start();
    }

    ///////////////////////////////////////////////////////////////////////////

    public void run() {
        log.debug(_sessionKey + " retrieval thread started");
        int incompleteListSize = _provider.getIncompleteListSize();
        CloseableIterator<String[]> iter = null;
        PrintWriter out = null;
        try {
            iter = _provider.getPathList();  // if empty, the impl should throw the right exception here
            _manager.addSession(_sessionKey, this);  // after this point, we depend on the session manager to clean up

            File sessionDir = new File(_baseDir, _sessionKey);
            sessionDir.mkdirs();

            int cursor = 0;

            // iterate all elements of the list...
            while (iter.hasNext() && !_threadNeedsToFinish) {
                File listFile = new File(sessionDir, _threadWorkingPart + ".txt");
                out = new PrintWriter(
                        new OutputStreamWriter(
                                new FileOutputStream(listFile)));
                for (int i = 0; i < incompleteListSize && iter.hasNext(); i++) {
                    String[] pathAndDate = iter.next();
                    out.print(pathAndDate[0]);            // path
                    if (pathAndDate.length > 1) {
                        out.print(" " + pathAndDate[1]);  // possibly date
                    }
                    out.println();
                }

                if (iter.hasNext()) {
                    int nextPartNum = _threadWorkingPart + 1;
                    String token = _sessionKey + "/" + nextPartNum;
                    out.println("end " + token + " " + cursor);
                    cursor += incompleteListSize;
                } else if (cursor > 0) {
                    out.println("end " + cursor);
                } else {
                    out.println("end");
                }
                out.close();
                _lastGeneratedPart++;
                log.debug("Successfully created file " + listFile.getPath());
                if (iter.hasNext()) {
                    _threadWorkingPart++;
                }
            }
        } catch (ServerException e) {
            _exception = e;
        } catch (Throwable th) {
            _exception = new ServerException("Unexpected error in session thread", th);
        } finally {
            if (iter != null) try {
                iter.close();
            } catch (Exception ignored) {
            }
            if (out != null) try {
                out.close();
            } catch (Exception ignored) {
            }
            _threadWorking = false;
            log.debug(_sessionKey + " retrieval thread finished");
        }
    }

    ///////////////////////////////////////////////////////////////////////////

    /**
     * Has the session expired?
     * <p/>
     * If this is true, the session will be cleaned by the reaper thread
     * of the session manager.
     */
    public boolean hasExpired() {
        if (_exception != null) {
            return true;
        }
        if (_lastGeneratedPart >= 0 && _lastSentPart > -1) {
            return _expirationTime < System.currentTimeMillis();
        }
        return false;
    }

    /**
     * Do all possible cleanup for this session.
     * <p/>
     * This includes signaling to its thread to stop asap,
     * waiting for it to stop, and removing any files/directories that remain.
     * <p/>
     * The implementation should be fail-safe, as a session may be asked to
     * clean itself more than once.
     * <p/>
     * Clean must *not* be called from this session's thread.
     */
    public void clean() {
        _threadNeedsToFinish = true;
        while (_threadWorking) {
            try {
                Thread.sleep(250);
            } catch (Exception ignored) {
            }
        }
        File sessionDir = new File(_baseDir, _sessionKey);
        if (sessionDir.exists()) {
            File[] files = sessionDir.listFiles();
            if (files != null) {
                log.debug(String.format(
                        "Deleting session %s directory and all %d files within",
                        _sessionKey, files.length));
                for (File file : files) {
                    file.delete();
                }
                sessionDir.delete();
            }
        }
    }

    /**
     * Get the named response, wait for it, or throw any exception that
     * has popped up while generating parts.
     */
    public ResponseData getResponseData(int partNum) throws ServerException {
        if (_exception != null) {
            throw _exception;
        }

        log.debug("Entered getResponseData(" + partNum + ")");

        // the current request should either be for the last sent part or plus one
        int nextPart = _lastSentPart + 1;
        if (partNum == _lastSentPart || partNum == nextPart) {

            // If the thread is still running and the last generated part was less than partNum,
            // wait till the thread is finished or the last generated part is greater or equal to partNum

            // Then, try to return the response
            while (_threadWorking && _lastGeneratedPart < partNum) {
                try {
                    Thread.sleep(100);
                } catch (Exception ignored) {
                }
            }
            if (_exception != null) {
                throw _exception;
            }
            File listFile = new File(_baseDir, _sessionKey + "/" + partNum + ".txt");
            if (!listFile.exists()) {
                StringBuffer message = new StringBuffer();
                message.append("the indicated part does not exist because ");
                message.append(listFile.getPath());
                message.append(" doesn't exist");
                File sessionDir = new File(_baseDir, _sessionKey);
                if (sessionDir.exists()) {
                    String[] names = sessionDir.list();
                    if (names.length == 0) {
                        message.append(".  In fact, the session directory "
                                + "is empty!  Has it expired unexpectedly?");
                    } else {
                        message.append(".  Only the following " + names.length
                                + " files currently exist in the session"
                                + " directory: ");
                        for (int i = 0; i < names.length; i++) {
                            if (i > 0) {
                                message.append(", ");
                            }
                            message.append(names[i]);
                        }
                    }
                } else {
                    message.append(".  In fact, the session directory "
                            + "doesn't even exist.  Has it expired unexpectedly?");
                }
                log.warn(message.toString());
                throw new BadResumptionTokenException("the indicated part does not exist");
            }
            String token = getResumptionToken(partNum + 1);
            ResponseData response = new ResponseDataImpl(
                    new CachedContentAggregate(listFile,
                            _provider.getVerb(),
                            _provider.getRecordCache()),
                    token);

            // Since we're going to succeed, make sure we clean up the previous part, if any
            if (partNum > 0) {
                int previousPart = partNum - 1;
                File toDelete = new File(_baseDir, _sessionKey + "/" + previousPart + ".txt");
                log.debug("Deleting previous part: " + toDelete.getPath());
                toDelete.delete();
            }

            _lastSentPart = partNum;
            _expirationTime = new Date().getTime() + (1000 * _secondsBetweenRequests);

            log.debug(String.format("%s returning part %d", _sessionKey, partNum));

            return response;
        } else {
            throw new BadResumptionTokenException("the indicated part either doesn't exist yet or has expired");
        }
    }

    private String getResumptionToken(int partNum) {
        if (_threadWorking) {
            if (_threadWorkingPart >= partNum) {
                return _sessionKey + "/" + partNum;
            } else {
                return null; // assume if it's not working on it or hasn't yet, there is no such part
            }
        } else {
            if (partNum <= _lastGeneratedPart) {
                return _sessionKey + "/" + partNum;
            } else {
                return null;
            }
        }
    }


    public void finalize() {
        clean();
    }

}
