package apexTest;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.BaseOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Collector extends BaseOperator implements Operator.CheckpointNotificationListener
{
    private long result;
    private static final Logger logger = LoggerFactory.getLogger(Collector.class);

    public transient DefaultInputPort<Long> input = new DefaultInputPort<Long>() {
        @Override
        public void process(Long aLong) {
            result = aLong;
        }
    };

    @Override
    public void setup(Context.OperatorContext context)
    {
        super.setup(context);
        logger.info("Setup {}", result);
    }

    @Override
    public void beginWindow(long windowId)
    {
        super.beginWindow(windowId);
    }

    @Override
    public void beforeCheckpoint(long l)
    {
        logger.info("Before checkpointing -- {} : {}\n", l, result);
    }

    @Override
    public void checkpointed(long l) {

    }

    @Override
    public void committed(long l)
    {
        logger.info("Collecter committed window Id {}\n", l);
    }
}
