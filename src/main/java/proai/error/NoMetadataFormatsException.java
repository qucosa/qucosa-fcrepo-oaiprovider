package proai.error;

public class NoMetadataFormatsException extends ProtocolException {

    public NoMetadataFormatsException() {
        super("the indicated item has no metadata formats");
    }

    public String getCode() {
        return "noMetadataFormats";
    }

}
