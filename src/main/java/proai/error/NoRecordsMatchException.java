package proai.error;

public class NoRecordsMatchException extends ProtocolException {

    public NoRecordsMatchException() {
        super("no records match your selection criteria");
    }

    public String getCode() {
        return "noRecordsMatch";
    }

}
