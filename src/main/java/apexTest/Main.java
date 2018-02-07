package apexTest;

import org.apache.apex.api.Launcher;
import org.apache.apex.api.YarnAppLauncher;
import org.apache.hadoop.conf.Configuration;

import javax.validation.ConstraintViolationException;

public class Main
{
    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration(true);
        conf.addResource("/META-INF/properties.xml");
        //YarnAppLauncher launcher = Launcher.getLauncher(Launcher.LaunchMode.YARN);
        Launcher launcher = Launcher.getLauncher(Launcher.LaunchMode.EMBEDDED);
        launcher.launchApp(new Application(), conf);
    }
}
