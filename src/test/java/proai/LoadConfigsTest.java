package proai;

import java.util.HashSet;

import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import proai.driver.impl.DisseminationTermsImpl;
import proai.driver.impl.SetSpecImpl;

public class LoadConfigsTest {
    private SetSpecImpl setSpecMerge = new SetSpecImpl();

    private DisseminationTermsImpl disseminationTermsImpl = new DisseminationTermsImpl();

    private ObjectMapper om = new ObjectMapper();

    @Test
    public void loadXpathDocNodes() {
        disseminationTermsImpl.getDissTerms();
    }

    @Test
    public void getSetSpecs() {
        setSpecMerge.setSetSpecs(new HashSet<String>());
        setSpecMerge.getSetSpecs();
    }
}
