using Nancy;
using Nancy.Configuration;
using Nancy.Hosting.Self;
using System;
using System.Threading;
using System.Collections.Generic;
using System.Configuration;
using System.Text;
using Topshelf;
using Topshelf.Runtime;

namespace NancyDataService
{
    class DataService : IDisposable
    {

        public DataService()
        {
        }

        private SemaphoreSlim _semaphoreToRequestStop;
        private Thread _thread;
        private NancyHost nancyHost = null;
        private Uri uri = null;
        private string msg;

        public void Start()
        {

            // start Nancy here
            uri = new Uri(ConfigurationManager.AppSettings["uri"]);
            var hostConfig = new HostConfiguration();
            hostConfig.UrlReservations.CreateAutomatically = true;
            hostConfig.RewriteLocalhost = false;

            try
            {
                nancyHost = new NancyHost(uri, new AppBootstrapper(), hostConfig);
                nancyHost.Start();
                msg = $"Nancy now listening on {uri}...";
                Console.WriteLine(msg);
                Logger.LogInfo(msg);

                // spin worker thread here
                _semaphoreToRequestStop = new SemaphoreSlim(0);
                _thread = new Thread(DoWork);
                _thread.Start(null);

            }
            catch (Exception ex)
            {
                msg = ex.Message;
                Logger.LogError(msg);
                Console.WriteLine($"Error: {msg}");
            }
        }
        private void DoWork(object obj)
        {
            if (nancyHost == null)
            {
                msg = "NancyHost could not be started.";
                Logger.LogError(msg);
                Console.WriteLine($"Error: {msg}");
            }
            else
            {
                // setup counter to display progress every x iterations
                int counter = 4;
                
                while (true)
                {
                    if (counter == 4)
                    {
                        Console.WriteLine(msg);
                        counter = 0;
                    }
                    if (_semaphoreToRequestStop.Wait(500))
                    {
                        //msg = "Stopping NancyDataService...";
                        //Console.WriteLine(msg);
                        //Logger.LogInfo(msg);
                        break;
                    }
                    ++counter;
                }
            }
        }

        public void Stop()
        {
            try
            {
                Logger.LogInfo("Stopping NancyDataService...");
                _semaphoreToRequestStop.Release();
                _thread.Join();
                nancyHost.Stop();
            }
            catch (Exception ex)
            {
                Logger.LogError(ex.Message);
                Console.WriteLine($"Error: {ex.Message}");
            }
            finally
            {
                nancyHost.Dispose();
            }
        }

        public void Dispose()
        {
            throw new NotImplementedException();
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
