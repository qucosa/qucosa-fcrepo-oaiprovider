package proai.cache;

public class QueueItem {

    private String _failDate;
    private String _failReason;
    private String _identifier;
    private String _mdPrefix;
    private int _queueKey;
    private char _queueSource;
    private ParsedRecord _rec;
    private String _sourceInfo;
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
