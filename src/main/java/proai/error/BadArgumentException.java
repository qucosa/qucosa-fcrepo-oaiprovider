package proai.error;

public class BadArgumentException extends ProtocolException {
    static final long serialVersionUID = 1;

    public BadArgumentException(String message) {
        super(message);
    }

    public BadArgumentException(String message,
                                Throwable cause) {
        super(message, cause);
    }

    public String getCode() {
        return "badArgument";
    }

}
