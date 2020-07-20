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

namespace NancyDataService
{
    class Program
    {
        //public static readonly ILog log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        static void Main(string[] args)
        {
            Logger.LogInfo("NancyDataService started...");
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
    public class AppBootstrapper : DefaultNancyBootstrapper
    {
        public override void Configure(INancyEnvironment environment)
        {
            environment.Tracing(enabled: false, displayErrorTraces: true);
        }
    }

}
