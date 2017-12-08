package oaiprovider.mappings;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;

/**
 * This is a json mapper class for mapping the list sets config file
 *
 * @author dseelig
 *
 */
@JsonRootName(value = "ListSets")
public class ListSetConfJson {
    @JsonProperty("sets")
    private List<Set> sets = new ArrayList<>();

    @JsonIgnoreProperties(value = { "setName" })
    public static class Set {
        @JsonProperty("setSpec")
        private String setSpec;

        @JsonProperty("setName")
        private String setName = null;

        @JsonProperty("predicate")
        private String predicate;

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

        public String getPredicate() {
            return predicate;
        }

        public void setPredicate(String predicate) {
            this.predicate = predicate;
        }
    }
}
