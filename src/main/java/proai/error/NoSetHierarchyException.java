package proai.error;

public class NoSetHierarchyException extends ProtocolException {
    static final long serialVersionUID = 1;

    public NoSetHierarchyException(String message) {
        super(message);
    }

    public String getCode() {
        return "noSetHierarchy";
    }

}
