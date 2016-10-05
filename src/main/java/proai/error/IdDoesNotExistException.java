package proai.error;

public class IdDoesNotExistException extends ProtocolException {

    public IdDoesNotExistException() {
        super("the indicated item does not exist");
    }

    public String getCode() {
        return "idDoesNotExist";
    }

}
