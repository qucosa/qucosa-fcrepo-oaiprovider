package oaiprovider.mappings;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DisseminationTerms {
    @JsonProperty("xDocNodes")
    private List<XpathDocNode> xDocNodes = new ArrayList<>();

    public List<XpathDocNode> getxDocNodes() {
        return xDocNodes;
    }

    public void setxDocNodes(List<XpathDocNode> xDocNodes) {
        this.xDocNodes = xDocNodes;
    }

    public static class XpathDocNode {
        @JsonProperty("name")
        private String name;

        @JsonProperty("term")
        private String term;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getTerm() {
            return term;
        }

        public void setTerm(String term) {
            this.term = term;
        }
    }
}
