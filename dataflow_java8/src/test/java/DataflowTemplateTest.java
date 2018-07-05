import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;


import java.util.*;

@RunWith(JUnit4.class)
public class DataflowTemplateTest {

    public interface MyPipelineOptions extends TestPipelineOptions, DataflowTemplate.MyPipelineOptions {}

    @Rule
    public TestPipeline p = TestPipeline.create();

    private static final TableRow EXPECTED_ROW =
            new TableRow()
                    .set("TLD", "4")
                    .set("TldRank", "2")
                    .set("Domain", "3")
                    .set("PrevRefIPs","12")
                    .set("PrevRefSubNets", "11")
                    .set("RefIPs","6")
                    .set("RefSubNets", "5")
                    .set("PrevGlobalRank","9")
                    .set("PrevTldRank","10")
                    .set("GlobalRank","1")
                    .set("IDN_TLD","8")
                    .set("IDN_Domain","7");

    private static final String INPUT_LINE = "1,2,3,4,5,6,7,8,9,10,11,12";

    @Test
    @Category(ValidatesRunner.class)
    public void stringToTableRow_shouldOutputTableRow() throws Exception {
        List<String> list = new ArrayList<>();
        list.add(INPUT_LINE);

        PCollection<String> output = p.apply(Create.of(list))
                .apply(new DataflowTemplate.StringToTableRow())
                .apply(ParDo.of(new TableRowUtils.rowAsStringTransform()));


        PAssert.that(output).containsInAnyOrder(TableRowUtils.rowAsString(EXPECTED_ROW));
        p.run().waitUntilFinish();
    }

}
