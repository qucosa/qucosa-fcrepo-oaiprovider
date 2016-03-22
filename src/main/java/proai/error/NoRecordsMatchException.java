package proai.error;

public class NoRecordsMatchException extends ProtocolException {
    static final long serialVersionUID = 1;

    public NoRecordsMatchException(String message) {
        super(message);
    }

    public String getCode() {
        return "noRecordsMatch";
    }

}
