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
  }

  @Override
  public void teardown() {
    logger.info("Final window id is {}", latestWindowId);
    logger.info("Last commited window id is {}", lastCommitedWindowId);
    logger.info("Last checkpointed window id is {}", lastCheckpointedWindowId);
    super.teardown();
  }

  @Override
  public void emitTuples()
  {
    if (count++ == 0) {
      out.emit(latestWindowId);
    }
  }

  @Override
  public void beforeCheckpoint(long l) {

  }

  @Override
  public void checkpointed(long l)
  {
    lastCheckpointedWindowId = l;
  }

  @Override
  public void committed(long l)
  {
    lastCommitedWindowId = l;
  }
}
