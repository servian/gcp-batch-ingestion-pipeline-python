import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

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
                    .set("PrefRefSubNets", "11")
                    .set("RefIPs","6")
                    .set("RefSubNets", "5")
                    .set("PrevGlobalRank","9")
                    .set("PrevTldRank","10")
                    .set("GlobalRank","1")
                    .set("IDN_TLD","8")
                    .set("IDN_Domain","7");

    @Test
    @Category(ValidatesRunner.class)
    public void stringToTableRow_shouldOutputTableRow() throws Exception {
        List<String> list = new ArrayList<>();
        list.add("1,2,3,4,5,6,7,8,9,10,11,12");
        PCollection<String> input = p.apply(Create.of(list));

        PCollection<TableRow> output = input.apply(new DataflowTemplate.StringToTableRow());
        PCollection<String> results = output.apply(ParDo.of(new FormatResults()));
        PAssert.that(results).containsInAnyOrder(canonicalFormat(EXPECTED_ROW));
        p.run().waitUntilFinish();
    }

    static String canonicalFormat(TableRow row) {
        List<String> entries = Lists.newArrayListWithCapacity(row.size());
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            entries.add(entry.getKey() + ":" + entry.getValue());
        }
        Collections.sort(entries);
        return Joiner.on(",").join(entries);
    }

    static class FormatResults extends DoFn<TableRow, String> {
        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            TableRow element = c.element();
            TableRow row =
                    new TableRow()
                            .set("TLD", element.get("TLD"))
                            .set("TldRank", element.get("TldRank"))
                            .set("Domain", element.get("Domain"))
                            .set("PrevRefIPs",element.get("PrevRefIPs"))
                            .set("PrefRefSubNets", element.get("PrevRefSubNets"))
                            .set("RefIPs",element.get("RefIPs"))
                            .set("RefSubNets", element.get("RefSubNets"))
                            .set("PrevGlobalRank",element.get("PrevGlobalRank"))
                            .set("PrevTldRank",element.get("PrevTldRank"))
                            .set("GlobalRank",element.get("GlobalRank"))
                            .set("IDN_TLD",element.get("IDN_TLD"))
                            .set("IDN_Domain",element.get("IDN_Domain"));
            c.output(canonicalFormat(row));
        }
    }

}
