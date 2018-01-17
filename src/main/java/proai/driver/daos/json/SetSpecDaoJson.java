package proai.driver.daos.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import oaiprovider.mappings.ListSetConfJson.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;

/**
 * Load the setspec json config, write in an set and return this set.
 *
 * @author dseelig
 *
 */
public class SetSpecDaoJson {
    private static final Logger logger = LoggerFactory.getLogger(SetSpecDaoJson.class);

    private List<Set> sets;

    public SetSpecDaoJson(InputStream setSpecsConfiguration) throws IOException {
        ObjectMapper om = new ObjectMapper();
        sets = om.readValue(setSpecsConfiguration, om.getTypeFactory().constructCollectionType(List.class, Set.class));
    }

    public List<Set> getSetObjects() {
        return sets;
    }

    public Set getSetObject(String setSpec) {
        Set setObj = null;

        for (Set obj : getSetObjects()) {

            if (obj.getSetSpec().equals(setSpec)) {
                setObj = obj;
                break;
            }
        }

        return setObj;
    }

    public java.util.Set<String> getSetSpecs() {
        java.util.Set<String> setSpecs = new HashSet<String>();

        for (int i = 0; i < getSetObjects().size(); i++) {
            Set set = getSetObjects().get(i);
            setSpecs.add(set.getSetSpec());
        }

        return setSpecs;
    }
}