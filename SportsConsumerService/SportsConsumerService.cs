using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace SportsConsumerService
{
    public class SportsConsumerService : BackgroundService
    {
        private ConsumerConfig _config;
        public SportsConsumerService()
        {
            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                ClientId = "Consumer 2",
                GroupId = "Bet API Sport Consumer",
                IsolationLevel = IsolationLevel.ReadCommitted,
                EnableAutoCommit = false,
                StatisticsIntervalMs = 10000
            };
            this._config = config;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            // New up a Producer class 
            var consumer = new ConsumerBuilder<string, string>(this._config)
                .SetErrorHandler(Handle_Error)
                .SetLogHandler(Handle_Log)
                .SetStatisticsHandler(Handle_Stats)
                .Build();

            consumer.Subscribe("betapisports");

            try
            {
                var commitCounter = 0;
                while (!stoppingToken.IsCancellationRequested)
                {
                    commitCounter += 1;
                    var consumeResult = consumer.Consume(stoppingToken);
                    var message = consumeResult.Message.Value;
                    Console.WriteLine($"Message Consumed: TPO={consumeResult.Topic}-{consumeResult.Partition}-{consumeResult.Offset}, Value={message.ToString()}");

                    if (commitCounter >= 10)
                    {
                        consumer.Commit(consumeResult);
                        commitCounter = 0;
                    }
                }
            }
            catch (OperationCanceledException)
            {


            }
            finally
            {
                consumer.Commit();
                consumer.Close();
            }
        }

        private static void Handle_Stats(IConsumer<string, string> consumer, string stats)
        {
            Console.WriteLine("====================");
            Console.WriteLine($"STATS: {stats}");
        }

        private static void Handle_Log(IConsumer<string, string> consumer, LogMessage logMessage)
        {
            Console.WriteLine("====================");
            Console.WriteLine($"LOG: {logMessage.Message}");
        }

        private static void Handle_Error(IConsumer<string, string> consumer, Error error)
        {
            Console.WriteLine("====================");
            Console.WriteLine($"ERROR: {error.Reason}");
        }
    }
}
