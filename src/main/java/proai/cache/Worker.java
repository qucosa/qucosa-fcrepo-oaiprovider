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

import net.sf.bvalid.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import proai.driver.OAIDriver;
import proai.util.StreamUtil;

import java.io.*;
import java.util.Iterator;
import java.util.List;

class Worker extends Thread {

    private static final Logger _LOG = LoggerFactory.getLogger(Worker.class);

    private final RCDisk _disk;
    private final OAIDriver _driver;
    private final Updater _updater;
    private final Validator _validator;
    private int _attemptedCount;
    private int _failedCount;
    private long _totalFetchTime;
    private long _totalValidationTime;

    public Worker(int num,
                  int of,
                  Updater updater,
                  OAIDriver driver,
                  RCDisk disk,
                  Validator validator) {
        super("Worker-" + num + "of" + of);
        _updater = updater;
        _driver = driver;
        _disk = disk;
        _validator = validator;
    }

    public void run() {

        _LOG.info("Worker started");

        List<QueueItem> queueItems = _updater.getNextBatch(null);

        while (queueItems != null && _updater.processingShouldContinue()) {

            Iterator<QueueItem> iter = queueItems.iterator();
            while (iter.hasNext() && _updater.processingShouldContinue()) {

                attempt(iter.next());
            }

            if (_updater.processingShouldContinue()) {
                queueItems = _updater.getNextBatch(queueItems);
            } else {
                _LOG.debug("About to finish prematurely because processing should stop");
            }
        }

        _LOG.info("Worker finished");
    }

    private void attempt(QueueItem qi) {

        RCDiskWriter diskWriter = null;
        long retrievalDelay = 0;
        long validationDelay = 0;
        try {

            diskWriter = _disk.getNewWriter();

            long startFetchTime = System.currentTimeMillis();
            _driver.writeRecordXML(qi.getIdentifier(),
                    qi.getMDPrefix(),
                    qi.getSourceInfo(),
                    diskWriter);
            diskWriter.flush();
            diskWriter.close();

            long endFetchTime = System.currentTimeMillis();

            retrievalDelay = endFetchTime - startFetchTime;

            if (_validator != null) {

                _validator.validate(getRecordStreamForValidation(diskWriter.getFile()),
                        RecordCache.OAI_SCHEMA_URL);
                validationDelay = System.currentTimeMillis() - endFetchTime;
            }

            qi.setParsedRecord(new ParsedRecord(qi.getIdentifier(),
                    qi.getMDPrefix(),
                    diskWriter.getPath(),
                    diskWriter.getFile()));

            qi.setSucceeded(true);

            _LOG.debug("Successfully processed record " + qi.getIdentifier());

        } catch (Throwable th) {

            _LOG.warn(String.format("Failed to process record %s: %s", qi.getIdentifier(), th.getMessage()));

            if (diskWriter != null) {
                diskWriter.close();
                diskWriter.getFile().delete();
            }

            StringWriter failReason = new StringWriter();
            th.printStackTrace(new PrintWriter(failReason, true));

            qi.setFailReason(failReason.toString());
            qi.setFailDate(StreamUtil.nowUTCString());
            _failedCount++;
        } finally {
            _attemptedCount++;
            _totalFetchTime += retrievalDelay;
            _totalValidationTime += validationDelay;
        }
    }

    private InputStream getRecordStreamForValidation(File recordFile) throws Exception {
        StringBuilder builder = new StringBuilder();
        builder.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        builder.append("<OAI-PMH xmlns=\"http://www.openarchives.org/OAI/2.0/\">\n");
        builder.append("<responseDate>2002-02-08T08:55:46Z</responseDate>\n");
        builder.append("<request verb=\"GetRecord\" identifier=\"oai:arXiv.org:cs/0112017\" ");
        builder.append("metadataPrefix=\"oai_dc\">http://arXiv.org/oai2</request>\n");
        builder.append("<GetRecord>\n");
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(recordFile)));
        String line = reader.readLine();
        while (line != null) {
            builder.append(line + "\n");
            line = reader.readLine();
        }
        builder.append("</GetRecord>\n");
        builder.append("</OAI-PMH>");
        return new ByteArrayInputStream(builder.toString().getBytes("UTF-8"));
    }

    public int getAttemptedCount() {
        return _attemptedCount;
    }

    public int getFailedCount() {
        return _failedCount;
    }

    public long getTotalFetchTime() {
        return _totalFetchTime;
    }

    public long getTotalValidationTime() {
        return _totalValidationTime;
    }
}
