package proai.error;

public class CannotDisseminateFormatException extends ProtocolException {

    public CannotDisseminateFormatException() {
        super("the metadataPrefix is unrecognized");
    }

    public String getCode() {
        return "cannotDisseminateFormat";
    }

}
