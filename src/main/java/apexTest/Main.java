package apexTest;

import org.apache.apex.api.Launcher;
import org.apache.apex.api.YarnAppLauncher;
import org.apache.hadoop.conf.Configuration;

import javax.validation.ConstraintViolationException;

public class Main
{
    public static void main(String[] args) throws Exception
    {
        YarnAppLauncher launcher = Launcher.getLauncher(Launcher.LaunchMode.YARN);
        //Launcher launcher = Launcher.getLauncher(Launcher.LaunchMode.EMBEDDED);
        Configuration conf = new Configuration(true);
        conf.addResource("/META-INF/properties.xml");
        //conf.set("_apex.originalAppId", "application_1516696268094_0559");
        launcher.launchApp(new Application(), conf);
    }
}
