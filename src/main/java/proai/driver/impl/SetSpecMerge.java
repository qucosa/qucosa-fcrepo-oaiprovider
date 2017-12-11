package proai.driver.impl;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import oaiprovider.mappings.DisseminationTerms;
import oaiprovider.mappings.DisseminationTerms.XpathDocNode;
import oaiprovider.mappings.ListSetConfJson.Set;

public class SetSpecMerge {
    private ObjectMapper om = new ObjectMapper();

    private String docXml;

    private java.util.Set setSpecs;

    public List<XpathDocNode> getXpathDocNodeTerms() {
        File dissTerms = new File(
                this.getClass().getClassLoader().getResource("config/dissemination-terms.json").getPath());
        DisseminationTerms terms = null;

        try {
            terms = om.readValue(dissTerms, DisseminationTerms.class);
        } catch (JsonParseException e) {
            e.printStackTrace();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return terms.getxDocNodes();
    }

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

    public String getDocXml() {
        return docXml;
    }

    public void setDocXml(String docXml) {
        this.docXml = docXml;
    }

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
