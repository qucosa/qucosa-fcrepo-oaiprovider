package proai.error;

public class BadResumptionTokenException extends ProtocolException {
    static final long serialVersionUID = 1;

    public BadResumptionTokenException(String message) {
        super(message);
    }

    public String getCode() {
        return "badResumptionToken";
    }

}
