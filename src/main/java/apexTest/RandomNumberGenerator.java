/**
 * Put your copyright and license info here.
 */
package apexTest;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.BaseOperator;

/**
 * This is a simple operator that emits random number.
 */
public class RandomNumberGenerator extends BaseOperator implements InputOperator, Operator.CheckpointNotificationListener
{
  private int numTuples = 100;
  private transient int count = 0;

  public final transient DefaultOutputPort<Double> out = new DefaultOutputPort<Double>();
  private long latestWindowId = 0;
  private long lastCommitedWindowId = 0;

  @Override
  public void setup(Context.OperatorContext context)
  {

    super.setup(context);
  }

  @Override
  public void beginWindow(long windowId)
  {
    count = 0;
    latestWindowId = windowId;
    System.out.println("\n----------windowID is " + windowId + "\n");
  }

  @Override
  public void teardown() {
    System.out.println("+++++++++++++Final window id is " + latestWindowId + "\n");
    System.out.println("+++++++++++++Last commited window id is " + lastCommitedWindowId + "\n\n\n");
    super.teardown();
  }

  @Override
  public void emitTuples()
  {
    if (count++ < numTuples) {
      out.emit(Math.random());
    }
  }

  @Override
  public void beforeCheckpoint(long l) {

  }

  public int getNumTuples()
  {
    return numTuples;
  }

  /**
   * Sets the number of tuples to be emitted every window.
   * @param numTuples number of tuples
   */
  public void setNumTuples(int numTuples)
  {
    this.numTuples = numTuples;
  }

  @Override
  public void checkpointed(long l)
  {
    System.out.println("\n?????????????windowID " + l + " is checkpointed\n");
  }

  @Override
  public void committed(long l)
  {
    lastCommitedWindowId = l;
    System.out.println("\n!!!!!!!!!!!windowID " + l + " is commited\n");
  }
}
