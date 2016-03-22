package proai.error;

public class NoMetadataFormatsException extends ProtocolException {
    static final long serialVersionUID = 1;

    public NoMetadataFormatsException(String message) {
        super(message);
    }

    public String getCode() {
        return "noMetadataFormats";
    }

}
