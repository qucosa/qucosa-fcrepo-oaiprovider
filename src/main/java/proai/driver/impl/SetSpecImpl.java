package proai.driver.impl;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;

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
public class SetSpecImpl {
    private ObjectMapper om = new ObjectMapper();

    /**
     * set with setspec entries from json setspec config file
     */
    private java.util.Set setSpecs;

    public List<Set> getSetSpecsConf() {
        List<Set> sets = null;
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

        return sets;
    }

    /**
     * returns a setspec set object with setspec json config entries
     *
     * @return {@link java.util.Set}
     */
    public java.util.Set<String> getSetSpecs() {
        setSpecs = new HashSet<>();

        for (int i = 0; i < getSetSpecsConf().size(); i++) {
            Set set = getSetSpecsConf().get(i);
            setSpecs.add(set.getSetSpec());
        }

        return setSpecs;
    }

    public void setSetSpecs(java.util.Set<String> setSpecs) {
        this.setSpecs = setSpecs;
    }
}
