/**
 * Put your copyright and license info here.
 */
package apexTest;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.BaseOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a simple operator that emits random number.
 */
public class RandomNumberGenerator extends BaseOperator implements InputOperator, Operator.CheckpointNotificationListener
{
  private int numTuples = 100;
  private transient int count = 0;
  private static final Logger logger = LoggerFactory.getLogger(RandomNumberGenerator.class);
  public final transient DefaultOutputPort<Long> out = new DefaultOutputPort<Long>();
  private long latestWindowId;
  private long lastCommitedWindowId;
  private long lastCheckpointedWindowId;
  private long commitedWindowCount;

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    commitedWindowCount = 0;
    logger.info("!!!Setup " + lastCheckpointedWindowId + "\n");
  }

  @Override
  public void beginWindow(long windowId)
  {
    count = 0;
    latestWindowId = windowId;
    logger.info("\n----------windowID is " + windowId + "\n");
  }

  @Override
  public void teardown() {
    logger.info("+++++++++++++Final window id is " + latestWindowId + "\n");
    logger.info("+++++++++++++Last commited window id is " + lastCommitedWindowId + "\n\n\n");
    logger.info("+++++++++++++Last checkpointed window id is " + lastCheckpointedWindowId + "\n\n\n");
    super.teardown();
  }

  @Override
  public void emitTuples()
  {
    if (count++ == 0) {
      out.emit(latestWindowId);
      logger.info("......Emitting " + latestWindowId + "\n");
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
    lastCheckpointedWindowId = l;
    logger.info("\n?????????????windowID " + l + " is checkpointed\n");
  }

  @Override
  public void committed(long l)
  {
    lastCommitedWindowId = l;
    logger.info("\n!!!!!!!!!!!windowID " + l + " is committed\n");
    logger.info("committed window count is {}", commitedWindowCount);
    if (commitedWindowCount++ == 5) {
      throw new ShutdownException();
    }
  }
}
