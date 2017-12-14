package proai;

import java.util.HashSet;

import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import proai.driver.impl.SetSpecMerge;

public class SetSpecConfigsTest {
    private SetSpecMerge setSpecMerge = new SetSpecMerge();

    private ObjectMapper om = new ObjectMapper();

    @Test
    public void loadXpathDocNodes() {
        setSpecMerge.getDissTerms();
    }

    @Test
    public void getSetSpecs() {
        setSpecMerge.setSetSpecs(new HashSet<String>());
        setSpecMerge.getSetSpecs();
    }
}
