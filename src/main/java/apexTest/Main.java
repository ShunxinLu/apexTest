package apexTest;

import org.apache.apex.api.Launcher;
import org.apache.apex.api.YarnAppLauncher;
import org.apache.hadoop.conf.Configuration;

public class Main
{
    public static void main(String[] args) throws Exception
    {
        YarnAppLauncher launcher = Launcher.getLauncher(Launcher.LaunchMode.YARN);
        Configuration conf = new Configuration(true);
        conf.addResource("/META-INF/properties.xml");

        /** Uncomment the following line and replace the "appId_for_app_to_restart" with actual app Id to restart
         * an application. Leave it as a comment will launch a new instance of the app.
         */
        //conf.set("_apex.originalAppId", appId_for_app_to_restart);

        launcher.launchApp(new Application(), conf);
    }
}
