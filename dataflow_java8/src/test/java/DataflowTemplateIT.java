import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DataflowTemplateIT {

    private static final String TABLE_SQL = "`gcp-batch-pattern.test_batch_servian.TopSitesIT`";
    private static final TableRow EXPECTED_ROW = new TableRow().set("f0_", "1000");

    @Test
    public void dataflowTemplatePipelineIT() {
        DataflowTemplate.MyPipelineOptions options = PipelineOptionsFactory.as(DataflowTemplate.MyPipelineOptions.class);
        options.setInputFile("gs://servian_melb_in_files/majestic_thousand.csv");
        options.setTableName("gcp-batch-pattern:test_batch_servian.TopSitesIT");
        options.setTempLocation("gs://servian_melb_practice/temp");

        /* Run the pipeline to be tested with option */
        DataflowTemplate.run(options);

        /* Run the pipeline to verify the contents in BQ created by the previous pipeline */
        Pipeline pipeline = Pipeline.create(options);
        PCollection<String> result = pipeline
                .apply(BigQueryIO.readTableRows().fromQuery("SELECT count(*) from " + TABLE_SQL).usingStandardSql())
                .apply(ParDo.of(new TableRowUtils.rowAsStringTransform()));

        /* Assert 1000 rows in table */
        PAssert.that(result).containsInAnyOrder(TableRowUtils.rowAsString(EXPECTED_ROW));
        pipeline.run().waitUntilFinish();

    }

}
