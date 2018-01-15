package proai.driver.daos.json;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import oaiprovider.mappings.ListSetConfJson.Set;

/**
 * Load the setspec json config, write in an set and return this set.
 *
 * @author dseelig
 *
 */
public class SetSpecDaoJson {
    private static final Logger logger = LoggerFactory.getLogger(SetSpecDaoJson.class);

    private List<Set> sets = null;

    public SetSpecDaoJson() {
        ObjectMapper om = new ObjectMapper();
        File setSpecs = new File(this.getClass().getClassLoader().getResource("config/list-set-conf.json").getPath());

        try {
            sets = om.readValue(setSpecs, om.getTypeFactory().constructCollectionType(List.class, Set.class));
        } catch (JsonParseException e) {
            e.printStackTrace();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public List<Set> getSetObjects() {
        return sets;
    }

    public Set getSetObject(String setSpec) {
        Set setObj = null;

        for (Set obj : getSetObjects()) {

            if (obj.getSetSpec().equals(setSpec)) {
                obj = setObj;
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