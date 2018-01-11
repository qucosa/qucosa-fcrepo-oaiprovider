package proai;

import java.util.HashSet;

import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import oaiprovider.mappings.DissTerms;
import proai.driver.impl.SetSpecImpl;

public class LoadConfigsTest {
    private SetSpecImpl setSpecMerge = new SetSpecImpl();

    private DissTerms dissTerms = new DissTerms();

    private ObjectMapper om = new ObjectMapper();

    @Test
    public void loadXpathDocNodes() {
        dissTerms.getDissTerms();
    }

    @Test
    public void getSetSpecs() {
        setSpecMerge.setSetSpecs(new HashSet<String>());
        setSpecMerge.getSetSpecs();
    }
}
