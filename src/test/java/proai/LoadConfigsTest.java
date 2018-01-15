package proai;

import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import oaiprovider.mappings.DissTerms;
import proai.driver.daos.json.SetSpecDaoJson;

public class LoadConfigsTest {
    private SetSpecDaoJson setSpecMerge = new SetSpecDaoJson();

    private DissTerms dissTerms = new DissTerms();

    private ObjectMapper om = new ObjectMapper();

    @Test
    public void loadXpathDocNodes() {
        dissTerms.getDissTerms();
    }

    @Test
    public void getSetSpecs() {
        setSpecMerge.getSetSpecs();
    }
}
