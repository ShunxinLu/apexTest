/**
 * Put your copyright and license info here.
 */
package apexTest;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

@ApplicationAnnotation(name = "ApexApplication")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // Sample DAG with 2 operators
    // Replace this code with the DAG you want to build

    SampleInputOperator sampleInput = dag.addOperator("sampleInput", SampleInputOperator.class);

    Collector collector = dag.addOperator("collector", new Collector());

    dag.addStream("randomData", sampleInput.out, collector.input);
  }
}
