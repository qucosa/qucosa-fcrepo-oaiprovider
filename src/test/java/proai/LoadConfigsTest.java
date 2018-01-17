package proai;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

import org.hamcrest.collection.IsEmptyCollection;
import org.junit.Assert;
import org.junit.Test;

import oaiprovider.mappings.ListSetConfJson.Set;
import proai.driver.daos.json.SetSpecDaoJson;

import java.io.IOException;

public class LoadConfigsTest {
    private SetSpecDaoJson setSpecMerge;

    public LoadConfigsTest() throws IOException {
        setSpecMerge = new SetSpecDaoJson(getClass().getResourceAsStream("/config/list-set-conf.json"));
    }

    @Test
    public void getSetSpecs() {
        Assert.assertNotNull(setSpecMerge.getSetSpecs());
        assertThat(setSpecMerge.getSetSpecs(), not(IsEmptyCollection.empty()));
    }

    @Test
    public void getSetSpec() {
        Set set = setSpecMerge.getSetObject("ddc:010");
        Assert.assertEquals("ddc:010", set.getSetSpec());
        Assert.assertEquals("Bibliography", set.getSetName());
        Assert.assertEquals("xDDC=010", set.getPredicate());
    }
}
