/* Notes:
 * - This application can run as a Console app or a Windows service.
 * - To run as a Console app:
 *   - Comment out the #define TOPSHELF line below.
 *   - Run from IDE (f5), or:
 *   - run built executable (...\NancyDataService\NancyDataService\bin\Release\netcoreapp3.0\win-x64\NancyDataService.exe)
 * - To run as a Windows service:
 *   - Service must be installed and running.
 *   - Make sure #define TOPSHELF line is NOT commented out.
 *   - To debug the service:
 *     - If the actual service is installed, make sure it is stopped.
 *     - Run from IDE (F5) to simulate the service, and allow debugging. Break points will work.
 * - To build the Windows service:
 *   - Build solution and make sure it compiles.
 *   - Open a command window.
 *   - Navigate to project folder (...\NancyDataService\NancyDataService).
 *   - Type: dotnet publish -r win-x64 -c Release {Enter}.
 * - To INSTALL, START, STOP or UNINSTALL:
 *   - Open an Administrator command window.
 *   - Navigate to publish folder (...\NancyDataService\NancyDataService\bin\Release\netcoreapp3.0\win-x64\publish)
 * - INSTALL:
 *   - Make sure it's not already installed. Verify in Services Console. If so, see uninstall instructions below.
 *   - Type: NancyDataService.exe install --localsystem -description "Select, Insert, Update, Delete from Access, MySql, Sql databases" {Enter}
 * - START:
 *   - Make sure it's not already started. Verify in Services Console. If so, you're done.
 *   - Type: NancyDataService start {Enter}.
 *     - This may time-out, but it should be started. Verify in Services Console.
 *     - You can also start or stop the service in the Services Console.
 * - STOP:
 *   - If it's not running already, you're done.
 *   - Type: NancyDataService stop {Enter}.
 * - UNINSTALL:
 *   - Make sure the service is stopped. Verify in Services Console.
 *   - Type: NancyDataService.exe uninstall {Enter}.
 * 
 * See other instructions at top of various modules and classes.
 */

#define TOPSHELF


using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.OleDb;
using Nancy;
using Nancy.Configuration;
using Nancy.Hosting.Self;

// need for log4net
using log4net;
using log4net.Config;
using System.Reflection;
using System.IO;
using Topshelf;
using Topshelf.Runtime;
namespace NancyDataService
{
    class Program
    {
        // moved this to Logger class.
        //public static readonly ILog log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);


        static void Main(string[] args)
        {
            string msg = "NancyDataService starting...";
            Logger.LogInfo(msg);

#if TOPSHELF

            var rc = HostFactory.Run(x =>
            {
                x.Service<DataService>(s =>
                {
                    s.ConstructUsing(name => new DataService());
                    s.WhenStarted(tc => tc.Start());
                    s.WhenStopped(tc => tc.Stop());
                    
                });
                x.OnException(ex => { Logger.LogError(ex.Message); });
                x.UseLog4Net("log4net.config");
                x.RunAsLocalSystem();
                x.StartAutomatically();
                x.EnableShutdown();
                x.EnableServiceRecovery(r => r.RestartService(TimeSpan.FromSeconds(10)));
                x.SetServiceName("NancyDataService");
            });
            var exitCode = (int)Convert.ChangeType(rc, rc.GetTypeCode());  //11
            Environment.ExitCode = exitCode;

#else

            var uri = new Uri(ConfigurationManager.AppSettings["uri"]);
            var hostConfig = new HostConfiguration();
            hostConfig.UrlReservations.CreateAutomatically = true;
            hostConfig.RewriteLocalhost = false;

            using (var nancyHost = new NancyHost(uri, new AppBootstrapper(), hostConfig))
            {
                try
                {
                    nancyHost.Start();
                    Console.WriteLine($"Nancy now listening on {uri}.\n\nPress any key to exit");
                    Logger.LogInfo($"Nancy now listening on {uri}...");
                }
                catch (Exception ex)
                {
                    Logger.LogError(ex.Message);
                    Console.WriteLine("Error " + ex.Message + "\n\nPress any key to exit");
                }
                Console.ReadKey();
                Logger.LogInfo("NancyDataService stopped...");
            }

#endif

        }

        public static void FindProvider()
        {
            var reader = OleDbEnumerator.GetRootEnumerator();

            var list = new List<String>();
            while (reader.Read())
            {
                for (var i = 0; i < reader.FieldCount; i++)
                {
                    if (reader.GetName(i) == "SOURCES_NAME")
                    {
                        list.Add(reader.GetValue(i).ToString());
                    }
                }
            }
            reader.Close();
            foreach (var provider in list)
            {
                if (provider.StartsWith("Microsoft.ACE.OLEDB"))
                {
                    Console.WriteLine(provider.ToString());
                    //this.provider = provider.ToString();
                }
            }
        }
    }
}
