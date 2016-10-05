package proai.error;

public class NoSetHierarchyException extends ProtocolException {

    public NoSetHierarchyException() {
        super("There are no sets in the repository.");
    }

    public String getCode() {
        return "noSetHierarchy";
    }

}
