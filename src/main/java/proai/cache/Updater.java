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

import static java.lang.String.format;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.httpclient.util.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sf.bvalid.Validator;
import oaiprovider.FedoraSetInfo;
import oaiprovider.mappings.ListSetConfJson;
import proai.MetadataFormat;
import proai.Record;
import proai.SetInfo;
import proai.driver.OAIDriver;
import proai.driver.RemoteIterator;
import proai.driver.daos.json.SetSpecDaoJson;
import proai.error.ImmediateShutdownException;
import proai.error.RepositoryException;
import proai.error.ServerException;
import proai.util.SetSpec;

public class Updater extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(Updater.class);

    private final RCDatabase _db;
    private final RCDisk _disk;
    private final OAIDriver _driver;
    private final int _maxCommitQueueSize;
    private final int _maxFailedRetries;
    private final int _maxRecordsPerTransaction;
    private final int _maxWorkBatchSize;
    private final int _maxWorkers;
    private final int _pollSeconds;
    private final Validator _validator;
    private Committer _committer;
    private boolean _immediateShutdownRequested;
    private boolean _processingAborted;
    private QueueIterator _queueIterator;
    private boolean _shutdownRequested;
    private String _status;
    private Worker[] _workers;

    public Updater(OAIDriver driver,
                   RCDatabase db,
                   RCDisk disk,
                   int pollSeconds,
                   int maxWorkers,
                   int maxWorkBatchSize,
                   int maxFailedRetries,
                   int maxCommitQueueSize,
                   int maxRecordsPerTransaction,
                   Validator validator) {
        _driver = driver;
        _db = db;
        _disk = disk;

        _pollSeconds = pollSeconds;
        _maxWorkers = maxWorkers;
        _maxWorkBatchSize = maxWorkBatchSize;
        _maxFailedRetries = maxFailedRetries;
        _maxCommitQueueSize = maxCommitQueueSize;
        _maxRecordsPerTransaction = maxRecordsPerTransaction;
        _validator = validator;
    }

    /**
     * For the given number of milliseconds, return a string like this:
     * <p/>
     * <p><pre>[h hours, ][m minutes, ]sec.ms seconds</pre>
     */
    private static String getHMSString(long ms) {

        StringBuffer out = new StringBuffer();

        long hours = ms / (1000 * 60 * 60);
        ms -= hours * 1000 * 60 * 60;
        long minutes = ms / (1000 * 60);
        ms -= minutes * 1000 * 60;
        long seconds = ms / 1000;
        ms -= seconds * 1000;

        if (hours > 0) {
            out.append(hours + " hours, ");
        }
        if (minutes > 0) {
            out.append(minutes + " minutes, ");
        }

        String msString;
        if (ms > 99) {
            msString = "." + ms;
        } else if (ms > 9) {
            msString = ".0" + ms;
        } else if (ms > 0) {
            msString = ".00" + ms;
        } else {
            msString = ".000";
        }

        out.append(seconds + msString + " seconds");

        return out.toString();
    }

    private static double round(double val) {
        return Math.round(val * 100.0) / 100.0;
    }

    @Override
    public void run() {

        _status = "Started";

        logger.info("Updater started");

        while (!_shutdownRequested) {

            long cycleStartTime = System.currentTimeMillis();

            logger.debug("Update cycle initiated");

            try {
                // It's important to do this first because old items may have
                // been left in the queue due to an immediate or improper
                // shutdown.  This ensures that unintentional duplicates
                // (especially old failures) aren't entered into the queue
                // during the polling+updating phase.
                _status = "Processing any old items in queue";
                checkImmediateShutdown();
                logger.debug("Processing old records in queue...");
                processQueue();

                checkImmediateShutdown();
                _status = "Polling and updating queue and database";
                pollAndUpdate();

                _status = "Processing any new items in queue";
                checkImmediateShutdown();
                logger.debug("Processing new records in queue...");
                processQueue();

                checkImmediateShutdown();
                _status = "Pruning old files from cache if needed";
                pruneIfNeeded();

                long sec = (System.currentTimeMillis() - cycleStartTime) / 1000;
                logger.debug(format("Update cycle finished in %dsec.Next cycle scheduled in %dsec.", sec, _pollSeconds));

            } catch (ImmediateShutdownException e) {
                logger.info("Update cycle aborted due to immediate shutdown request");
            } catch (Throwable th) {
                logger.error("Update cycle failed", th);
            }

            _status = "Sleeping";
            int waitedSeconds = 0;
            while (!_shutdownRequested && waitedSeconds < _pollSeconds) {
                try {
                    Thread.sleep(1000);
                } catch (Exception ignored) {
                }
                waitedSeconds++;
            }

        }
        _status = "Finished";

    }

    private void checkImmediateShutdown() throws ImmediateShutdownException {
        if (_immediateShutdownRequested) {
            throw new ImmediateShutdownException();
        }
    }

    /**
     * Signal that the thread should be shut down and wait for it to finish.
     * <p/>
     * If immediate is true, abort the update cycle if it's running.
     */
    public void shutdown(boolean immediate) {

        if (this.isAlive()) {

            _shutdownRequested = true;
            _immediateShutdownRequested = immediate;

            logger.info(format("Waiting for updater to finish.  Current status: %s", _status));
            while (this.isAlive()) {
                try {
                    Thread.sleep(250);
                } catch (Exception ignored) {
                }
            }
            logger.info("Updater shutdown complete");
        }
    }

    /**
     * Handle an exception encountered by currently-running Committer while
     * committing.
     */
    protected void handleCommitException(Throwable th) {
        logger.warn("Processing aborted due to commit failure", th);
        _processingAborted = true;
    }

    // return null if no more batches or processing should stop
    protected List<QueueItem> getNextBatch(List<QueueItem> finishedItems) {

        List<QueueItem> nextBatch = null;

        if (!processingShouldStop()) {

            if (finishedItems != null) {
                _committer.handoff(finishedItems);
            }

            try {
                synchronized (_queueIterator) {
                    if (_queueIterator.hasNext()) {
                        nextBatch = new ArrayList<>();
                        while (_queueIterator.hasNext() &&
                                nextBatch.size() < _maxWorkBatchSize) {
                            nextBatch.add(_queueIterator.next());
                        }
                    }
                }
            } catch (Throwable th) {
                logger.warn("Processing aborted due to commit failure", th);
                synchronized (this) {
                    _processingAborted = true;
                }
                nextBatch = null;
            }

        }

        return nextBatch;
    }

    private int countItemsInQueue() throws Exception {
        Connection conn = RecordCache.getConnection();
        try {
            return _db.getQueueSize(conn);
        } finally {
            RecordCache.releaseConnection(conn);
        }
    }

    /**
     * Get a new <code>QueueIterator</code> over the current queue.
     */
    private QueueIterator newQueueIterator() throws Exception {

        Connection conn = null;
        File queueFile = null;
        PrintWriter queueWriter = null;
        try {

            conn = RecordCache.getConnection();

            queueFile = File.createTempFile("proai-queue", ".txt");
            queueWriter = new PrintWriter(
                    new OutputStreamWriter(
                            new FileOutputStream(queueFile), "UTF-8"));
            _db.dumpQueue(conn, queueWriter);
            queueWriter.close();

            return new QueueIterator(queueFile);

        } finally {
            if (queueWriter != null) {
                try {
                    queueWriter.close();
                } catch (Exception ignored) {
                }
            }
            if (queueFile != null) {
                queueFile.delete();
            }
            RecordCache.releaseConnection(conn);
        }

    }

    /**
     * Process the queue till it's empty.
     */
    private void processQueue() throws Exception {

        int itemsInQueue = countItemsInQueue();

        checkImmediateShutdown();
        if (itemsInQueue > 0) {

            long processingStartTime = System.currentTimeMillis();
            _processingAborted = false;

            while (itemsInQueue > 0 && !_processingAborted) {

                try {

                    _queueIterator = newQueueIterator();

                    // the committer must exist before the workers are started
                    _committer = new Committer(this,
                            _db,
                            _maxCommitQueueSize,
                            _maxRecordsPerTransaction);

                    // decide how many workers to create (1 to _maxWorkers)
                    int numWorkers = itemsInQueue / _maxWorkBatchSize;
                    if (numWorkers > _maxWorkers) numWorkers = _maxWorkers;
                    if (numWorkers == 0) numWorkers = 1;

                    logger.debug(format(
                            "Queue has %d records.  Starting %d worker threads for processing.",
                            itemsInQueue, numWorkers));

                    // start the workers
                    _workers = new Worker[numWorkers];
                    for (int i = 0; i < _workers.length; i++) {
                        _workers[i] = new Worker(i + 1,
                                _workers.length,
                                this,
                                _driver,
                                _disk,
                                _validator);
                        _workers[i].start();
                    }

                    // the workers must exist before the committer is started
                    _committer.start();

                    // wait for workers and committer to finish
                    while (_committer.isAlive()) {
                        try {
                            Thread.sleep(1000);
                        } catch (Exception ignored) {
                        }
                    }

                    checkImmediateShutdown();

                } finally {

                    // clean up and log stats for this round of processing
                    if (_queueIterator != null) {
                        _queueIterator.close();
                    }

                    if (_workers != null) {
                        logProcessingStats(itemsInQueue,
                                System.currentTimeMillis() - processingStartTime);
                        _workers = null;
                        _committer = null;
                    }
                }

                itemsInQueue = countItemsInQueue();
            }

            if (_processingAborted) {
                throw new ServerException("Queue processing was aborted due to unexpected error");
            }

        } else {
            logger.debug("Queue is empty.  No processing needed.");
        }

    }

    private void pollAndUpdate() throws ServerException {

        Connection conn = null;
        boolean startedTransaction = false;
        try {
            conn = RecordCache.getConnection();
            conn.setAutoCommit(false);
            startedTransaction = true;

            _db.queueFailedRecords(conn, _maxFailedRetries);

            if (_db.isPollingEnabled(conn)) {
                Date latestRemoteDate = _driver.getLatestDate();
                Date earliestPollDate = new Date(_db.getEarliestPollDate(conn));

                logger.debug(String.format("Latest modification reported by Fedora: %s",
                        DateUtil.formatDate(latestRemoteDate)));
                logger.debug(String.format("Earliest poll date: %s",
                        DateUtil.formatDate(earliestPollDate)));

                if (latestRemoteDate.after(earliestPollDate)) {

                    logger.debug("Starting update process; source data of interest may have changed.");
                    checkImmediateShutdown();
                    updateIdentify(conn);

                    checkImmediateShutdown();
                    List<String> allPrefixes = updateFormats(conn);

                    checkImmediateShutdown();
                    updateSets(conn);

                    checkImmediateShutdown();
                    queueUpdatedRecords(conn, allPrefixes, latestRemoteDate);
                } else {
                    logger.debug("Skipping update process; source data of interest has not changed");
                }
            } else {
                logger.debug("Remote polling skipped -- polling is disabled");
            }

            conn.commit();
        } catch (Throwable th) {
            if (startedTransaction) {
                try {
                    conn.rollback();
                } catch (SQLException e) {
                    logger.error("Failed to roll back failed transaction", e);
                }
            }
            throw new ServerException("Update cycle phase one aborted", th);
        } finally {
            if (conn != null) {
                try {
                    if (startedTransaction) conn.setAutoCommit(false);
                } catch (SQLException e) {
                    logger.error("Failed to set autoCommit to false", e);
                } finally {
                    RecordCache.releaseConnection(conn);
                }
            }
        }

    }

    private void pruneIfNeeded() throws Exception {

        Connection conn = null;
        File resultFile = null;
        PrintWriter resultWriter = null;
        BufferedReader resultReader = null;

        try {
            conn = RecordCache.getConnection();

            if (_db.getPrunableCount(conn) > 0) {

                resultFile = File.createTempFile("proai-prunable", ".txt");
                resultWriter = new PrintWriter(
                        new OutputStreamWriter(
                                new FileOutputStream(resultFile), "UTF-8"));

                int numToPrune = _db.dumpPrunables(conn, resultWriter);
                resultWriter.close();

                logger.debug("Pruning " + numToPrune + " old files from cache");
                resultReader = new BufferedReader(
                        new InputStreamReader(
                                new FileInputStream(resultFile), "UTF-8"));

                int i = 0;
                int[] toPruneKeys = new int[32];

                String line = resultReader.readLine();

                while (line != null) {

                    String[] parts = line.split(" ");
                    if (parts.length == 2) {

                        int pruneKey = Integer.parseInt(parts[0]);
                        File file = _disk.getFile(parts[1]);

                        if (file.exists()) {
                            boolean deleted = file.delete();
                            if (deleted) {
                                logger.debug("Deleted old cache file: " + parts[1]);
                            } else {
                                logger.warn("Unable to delete old cache file (will try again later): " + parts[1]);
                            }
                        } else {
                            logger.debug("No need to delete non-existing old cache file: " + parts[1]);
                        }

                        // delete from prune list if it no longer exists
                        toPruneKeys[i++] = pruneKey;
                        if (i == toPruneKeys.length) {
                            _db.deletePrunables(conn, toPruneKeys, i);
                            i = 0;
                        }
                    }

                    line = resultReader.readLine();
                }

                // do final chunk if needed
                if (i > 0) {
                    _db.deletePrunables(conn, toPruneKeys, i);
                }
            } else {
                logger.debug("Pruning is not needed.");
            }

        } finally {
            if (resultWriter != null) {
                try {
                    resultWriter.close();
                } catch (Exception ignored) {
                }
                if (resultReader != null) {
                    try {
                        resultReader.close();
                    } catch (Exception ignored) {
                    }
                }
            }
            if (resultFile != null) {
                resultFile.delete();
            }
            RecordCache.releaseConnection(conn);
        }

    }

    /**
     * Log stats for a round of processing.
     * <p/>
     * This assumes the array of workers and the committer have been
     * initialized.
     */
    private void logProcessingStats(int initialQueueSize,
                                    long totalDuration) {

        int recordsProcessed = _committer.getProcessedCount();
        double processingRate = recordsProcessed / (totalDuration / 1000.0);

        int failedCount = 0;
        int attemptedCount = 0;
        long totalFetchTime = 0;
        for (Worker _worker : _workers) {
            failedCount += _worker.getFailedCount();
            attemptedCount += _worker.getAttemptedCount();
            totalFetchTime += _worker.getTotalFetchTime();
        }
        long msPerAttempt = totalFetchTime / attemptedCount;
        int transactionCount = _committer.getTransactionCount();
        long msPerTrans = Math.round((double) _committer.getTotalCommitTime() / (double) transactionCount);
        double recsPerTrans = (double) recordsProcessed / (double) transactionCount;

        StringBuilder stats = new StringBuilder()
                .append(format("\tRecords processed\t\t\t: %d of %d on queue\n", recordsProcessed, initialQueueSize))
                .append(format("\tTotal processing time\t\t\t: %s\n", getHMSString(totalDuration)))
                .append(format("\tProcessing rate\t\t\t: %s records/second\n", round(processingRate)))
                .append(format("\tWorkers spawned\t\t\t: %d of %d maximum\n", _workers.length, _maxWorkers))
                .append(format("\tFailed record loads\t\t\t: %d of %d attempted\n", failedCount, attemptedCount))
                .append(format("\tAvg roundtrip fetch time\t\t\t: %s\n", getHMSString(msPerAttempt)))
                .append(format("\tTotal DB transactions\t\t\t: %d\n", transactionCount))
                .append(format("\tTotal transaction time\t\t\t: %s\n", getHMSString(_committer.getTotalCommitTime())))
                .append(format("\tAvg time/transaction\t\t\t: %s\n", getHMSString(msPerTrans)))
                .append(format("\tAvg recs/transaction\t\t\t: %s of %d maximum\n", round(recsPerTrans), _maxRecordsPerTransaction));

        logger.info(format("A round of queue processing has finished.\n\nProcessing Stats:\n%s", stats.toString()));
    }

    /**
     * Update all formats and return the latest list of mdPrefixes.
     * <p/>
     * <p>This will add any new formats, modify any changed formats,
     * and delete any no-longer-existing formats (and associated records).
     */
    private List<String> updateFormats(Connection conn) {

        logger.debug("Updating metadata formats...");

        // apply new / updated
        RemoteIterator<? extends MetadataFormat> riter = _driver.listMetadataFormats();
        List<String> newPrefixes = new ArrayList<>();
        try {
            while (riter.hasNext()) {

                checkImmediateShutdown();
                MetadataFormat format = riter.next();
                _db.putFormat(conn, format);
                newPrefixes.add(format.getPrefix());
            }
        } finally {
            try {
                riter.close();
            } catch (Exception e) {
                logger.warn("Unable to close remote metadata format iterator", e);
            }
        }

        // apply deleted
        for (CachedMetadataFormat format : _db.getFormats(conn)) {

            String oldPrefix = format.getPrefix();
            if (!newPrefixes.contains(oldPrefix)) {

                checkImmediateShutdown();
                _db.deleteFormat(conn, oldPrefix);
            }
        }

        return newPrefixes;
    }

    private void updateIdentify(Connection conn) {

        logger.debug("Getting 'Identify' xml from remote source...");

        _db.setIdentifyPath(conn, _disk.write(_driver));
    }

    /**
     * Update all sets.
     * <p/>
     * <p>This will add any new sets, modify any changed sets, and delete any
     * no-longer-existing sets (and associated membership data).
     */
    private void updateSets(Connection conn) {

        logger.debug("Updating sets...");

        // apply new / updated
        RemoteIterator<? extends SetInfo> riter = _driver.listSetInfo();
        Set<SetInfo> setInfos = new HashSet<>();

        try {

            while (riter.hasNext()) {
                setInfos.add(riter.next());
            }
        } finally {

            try {
                riter.close();
            } catch (Exception e) {
                logger.warn("Unable to close remote set info iterator", e);
            }
        }

        // add sets from json config
        for (ListSetConfJson.Set set : ((SetSpecDaoJson) _driver.getProps().get("dynSetSpecs")).getSetObjects()) {
            FedoraSetInfo fedoraSetInfo = new FedoraSetInfo();
            fedoraSetInfo.setSpec(set.getSetSpec());
            fedoraSetInfo.setName(set.getSetName());
            setInfos.add(fedoraSetInfo);
        }

        Set<String> newSpecs = new HashSet<>();
        Set<String> missingSpecs = new HashSet<>();

        for (SetInfo setInfo : setInfos) {
                checkImmediateShutdown();
                String encounteredSetSpec = setInfo.getSetSpec();

                /*
                 * If we encounter a setSpec that implies that it is
                 * a subset, look for the parent.  If we haven't
                 * encountered its parent yet, remember its identity:
                 * unless we encounter it in subsequent results, we'll
                 * have to use a default placeholder for it later.
                 */
                if (SetSpec.hasParents(encounteredSetSpec) &&
                        !newSpecs.contains(
                                SetSpec.parentOf(encounteredSetSpec))) {
                    missingSpecs.add(SetSpec.parentOf(encounteredSetSpec));
                }
                _db.putSetInfo(
                        conn, encounteredSetSpec, _disk.write(setInfo));
                newSpecs.add(encounteredSetSpec);
        }

        /* Add any sets that are IMPLIED to exist, but weren't defined */
        for (String possiblyMissing : missingSpecs) {

            if (!SetSpec.isValid(possiblyMissing)) {
                throw new RepositoryException(format("SetSpec '%s' is malformed", possiblyMissing));
            }

            for (String spec : SetSpec.allSetsFor(possiblyMissing)) {
                if (!newSpecs.contains(spec)) {
                    _db.putSetInfo(conn, spec, _disk.write(
                            SetSpec.defaultInfoFor(spec)));
                    newSpecs.add(spec);
                    logger.info(format("Adding missing set: %s", spec));
                }
            }
        }

        // apply deleted
        for (SetInfo setInfo : _db.getSetInfo(conn)) {

            String oldSpec = setInfo.getSetSpec();
            if (!newSpecs.contains(oldSpec)) {

                checkImmediateShutdown();
                _db.deleteSet(conn, oldSpec);
            }
        }
    }

    private void queueUpdatedRecords(Connection conn,
                                     List<String> allPrefixes,
                                     Date latestRemoteDate) {

        logger.debug("Querying and queueing updated records...");

        long queueStartTime = System.currentTimeMillis();
        int totalQueuedCount = 0;
        for (String mdPrefix : allPrefixes) {

            Date lastPollDate = new Date(_db.getLastPollDate(conn, mdPrefix));

            // if something may have changed remotely *after* the last
            // known date that any records of this format were queried for,
            // query for updated records
            if (lastPollDate.before(latestRemoteDate)) {

                logger.debug(format(
                        "Querying for changed %s records because %d is less than %d",
                        mdPrefix, lastPollDate.getTime(), latestRemoteDate.getTime()));

                checkImmediateShutdown();
                RemoteIterator<? extends Record> riter = _driver.listRecords(
                        lastPollDate,
                        latestRemoteDate,
                        mdPrefix);
                try {

                    int queuedCount = 0;

                    while (riter.hasNext()) {

                        Record record = riter.next();
                        checkImmediateShutdown();
                        _db.queueRemoteRecord(conn,
                                record.getItemID(),
                                record.getPrefix(),
                                record.getSourceInfo());
                        queuedCount++;
                    }

                    logger.debug(format(
                            "Queued %d new/modified %s records.",
                            queuedCount, mdPrefix));

                    _db.setLastPollDate(conn, mdPrefix, latestRemoteDate);

                    totalQueuedCount += queuedCount;
                } finally {
                    try {
                        riter.close();
                    } catch (Exception e) {
                        logger.debug("Unable to close remote record iterator", e);
                    }
                }
            } else {
                logger.debug(format(
                        "Skipping %s records because %d is not less than %d",
                        mdPrefix, lastPollDate.getTime(), latestRemoteDate.getTime()));
            }
        }

        long sec = (System.currentTimeMillis() - queueStartTime) / 1000;
        logger.debug(format(
                "Queued %d total new/modified records in %dsec.",
                totalQueuedCount, sec));
    }

    protected synchronized boolean processingShouldStop() {
        return _processingAborted || _immediateShutdownRequested;
    }

    protected boolean anyWorkersAreRunning() {
        if (_workers == null) {
            return false;
        } else {
            for (Worker _worker : _workers) {
                if (_worker.isAlive()) return true;
            }
            return false;
        }
    }
}
