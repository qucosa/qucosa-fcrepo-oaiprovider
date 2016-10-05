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

public class QueueItem {

    private final String _identifier;
    private final String _mdPrefix;
    private final int _queueKey;
    private final char _queueSource;
    private final String _sourceInfo;
    private String _failDate;
    private String _failReason;
    private ParsedRecord _rec;
    private boolean _succeeded;

    public QueueItem(int queueKey,
                     String identifier,
                     String mdPrefix,
                     String sourceInfo,
                     char queueSource) {
        _queueKey = queueKey;
        _identifier = identifier;
        _mdPrefix = mdPrefix;
        _sourceInfo = sourceInfo;
        _queueSource = queueSource;
    }

    public int getQueueKey() {
        return _queueKey;
    }

    public String getIdentifier() {
        return _identifier;
    }

    public String getMDPrefix() {
        return _mdPrefix;
    }

    public String getSourceInfo() {
        return _sourceInfo;
    }

    public char getQueueSource() {
        return _queueSource;
    }

    public boolean succeeded() {
        return _succeeded;
    }

    public void setSucceeded(boolean succeeded) {
        _succeeded = succeeded;
    }

    public String getFailReason() {
        return _failReason;
    }

    public void setFailReason(String failReason) {
        _failReason = failReason;
    }

    public String getFailDate() {
        return _failDate;
    }

    public void setFailDate(String failDate) {
        _failDate = failDate;
    }

    public ParsedRecord getParsedRecord() {
        return _rec;
    }

    public void setParsedRecord(ParsedRecord rec) {
        _rec = rec;
    }
}
