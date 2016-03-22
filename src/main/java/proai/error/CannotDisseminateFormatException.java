package proai.error;

public class CannotDisseminateFormatException extends ProtocolException {
    static final long serialVersionUID = 1;

    public CannotDisseminateFormatException(String message) {
        super(message);
    }

    public String getCode() {
        return "cannotDisseminateFormat";
    }

}
