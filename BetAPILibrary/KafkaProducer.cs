using BetAPILibrary.Models;
using Confluent.Kafka;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace BetAPILibrary
{
    public class KafkaProducer
    {
        private ProducerConfig _config;
        public KafkaProducer()
        {
            this._config = new ProducerConfig()
            {
                BootstrapServers = "localhost:9092",
                ClientId = "Producer 1",
                Acks = Acks.All,
                Partitioner = Partitioner.Murmur2Random,
                CompressionType = CompressionType.Lz4,
                LingerMs = 50,
                BatchNumMessages = 100,
                EnableIdempotence = true,
                StatisticsIntervalMs = 10000
            };
        }
        public void SendMessageToKafka(List<SyXSport> sports)
        {
            //Producer
            var producer = new ProducerBuilder<string, string>(this._config)
                .SetErrorHandler(HandlerError)
                .SetLogHandler(HandleLogs)
                .SetStatisticsHandler(HandleStats)
                .Build();
            foreach (var sport in sports)
            {
                var message = new Message<string, string>();
                message.Key = $"Key-{sport.Name}";
                message.Value = JsonConvert.SerializeObject(sport);

                if(sport.Name.Equals("Soccer"))
                {
                    producer.Produce("sport-soccer", message, dr =>
                    {
                        if (!dr.Error.IsError)
                        {
                            Console.WriteLine($"P[{dr.Partition}]O[{dr.Offset}]");
                        }
                    });
                } else
                {
                    producer.Produce("betapisports", message, dr =>
                    {
                        if (!dr.Error.IsError)
                        {
                            Console.WriteLine($"P[{dr.Partition}]O[{dr.Offset}]");
                        }
                    });
                }

            }

            producer.Flush(TimeSpan.FromSeconds(10));

            while (true)
            {
                System.Threading.Thread.Sleep(100);
            }
        }

        private  void HandleStats(IProducer<string, string> arg1, string stats)
        {
            Console.WriteLine(stats);
        }

        private static void HandleLogs(IProducer<string, string> arg1, LogMessage arg2)
        {
            throw new NotImplementedException();
        }

        private static void HandlerError(IProducer<string, string> arg1, Error arg2)
        {
            throw new NotImplementedException();
        }
    }
}
