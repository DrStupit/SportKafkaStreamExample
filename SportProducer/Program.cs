using BetAPILibrary;
using Confluent.Kafka;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using RestSharp;
using SportProducer.Models;
using System;
using System.Collections.Generic;

namespace SportProducer
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Bet API Sport");
            //var listOfSports = new BetApiRequests().GetSyXSports();
            new KafkaProducer().SendMessageToKafka(new BetApiRequests().GetSyXSports());
        }


    }
}
