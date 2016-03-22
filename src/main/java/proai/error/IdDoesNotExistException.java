package proai.error;

public class IdDoesNotExistException extends ProtocolException {
    static final long serialVersionUID = 1;

    public IdDoesNotExistException(String message) {
        super(message);
    }

    public String getCode() {
        return "idDoesNotExist";
    }

}
