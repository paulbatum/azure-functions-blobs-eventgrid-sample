using System;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;

namespace FuncBlobs.LoadGenerator
{
    public static class LoadTimer
    {
        [FunctionName("LoadTimer")]
        public static async Task Run(
            [TimerTrigger("*/10 * * * * *")]TimerInfo myTimer,
            [Queue("%LoadQueueName%", Connection = "AzureWebJobsStorage")] IAsyncCollector<LoadJob> loadJobs,
            ILogger log)
        {
            var batchCount = int.Parse(Environment.GetEnvironmentVariable("BatchCount") ?? "10");
            var batchSize = int.Parse(Environment.GetEnvironmentVariable("BatchSize") ?? "10");
            var containerName = Environment.GetEnvironmentVariable("ContainerName") ?? "container01";
            var accountName = Environment.GetEnvironmentVariable("AccountName") ?? "pbatumfuncblobwus2acc01";

            var loadJob = new LoadJob
            {
                Account = accountName,
                Container = containerName,
                Size = batchSize
            };

            var bufferBlock = new BufferBlock<LoadJob>();
            var addMessageBlock = new ActionBlock<LoadJob>(async job =>
            {
                await loadJobs.AddAsync(job);
            }, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 32 });

            bufferBlock.LinkTo(addMessageBlock,
                new DataflowLinkOptions { PropagateCompletion = true });

            for (int i = 0; i < batchCount; i++)
                bufferBlock.Post(loadJob);

            bufferBlock.Complete();
            await addMessageBlock.Completion;
        }
    }
}
