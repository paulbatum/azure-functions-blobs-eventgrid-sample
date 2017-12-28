using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.ServiceBus;

namespace FuncBlobs.EventWriter
{
    public static class EventWriter
    {
        [FunctionName("EventWriter")]
        public static void Run([EventHubTrigger("%HubName%", Connection = "EventsConnectionString")] string myEventHubMessage, TraceWriter log)
        {
            log.Info($"C# Event Hub trigger function processed a message: {myEventHubMessage}");
        }
    }
}
