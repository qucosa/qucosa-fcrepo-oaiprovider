package proai.error;

public class BadVerbException extends ProtocolException {
    static final long serialVersionUID = 1;

    public BadVerbException(String message) {
        super(message);
    }

    public String getCode() {
        return "badVerb";
    }

}
