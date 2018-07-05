import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DataflowTemplateIT {

    public interface MyPipelineOptions extends TestPipelineOptions, DataflowTemplate.MyPipelineOptions {}

    @BeforeClass
    public static void setUp() {
        PipelineOptionsFactory.register(TestPipelineOptions.class);
    }

    @Test
    public void test() {
        MyPipelineOptions options = TestPipeline.testingPipelineOptions().as(MyPipelineOptions.class);
        options.setInputFile("gs://servian_melb_in_files/majestic_thousand.csv");
        options.setTableName("gcp-batch-pattern:test_batch_servian.TopSitesIT");
        options.setTempLocation("gs://servian_melb_practice/temp");
        DataflowTemplate.run(options);
    }
}
