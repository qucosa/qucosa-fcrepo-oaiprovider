package oaiprovider.mappings;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ListSetConfJson {

    @JsonProperty("setSpec")
    private String setSpec;

    @JsonProperty("setName")
    private String setName = null;

    public String getSetSpec() {
        return setSpec;
    }

    public void setSetSpec(String setSpec) {
        this.setSpec = setSpec;
    }

    public String getSetName() {
        return setName;
    }

    public void setSetName(String setName) {
        this.setName = setName;
    }

}
