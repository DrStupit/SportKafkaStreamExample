using BetAPILibrary.Models;
using Newtonsoft.Json;
using RestSharp;
using System;
using System.Collections.Generic;

namespace BetAPILibrary
{
    public class BetApiRequests
    {
        public List<SyXSport> GetSyXSports()
        {
            var client = new RestClient("https://betapistaging.hollywoodbets.net");
            var request = new RestRequest("api/sports", Method.GET);

            var response = client.Execute<Sport>(request);

            var sportList = new List<SyXSport>();

            var serializedSportObject = JsonConvert.DeserializeObject<Sport>(response.Content);

            foreach (var sport in serializedSportObject.responseObject)
            {
                sportList.Add(sport);


            }
            return sportList;
        }

        public List<Countries> GetCountryBySport(int sportId)
        {
            var client = new RestClient("https://betapistaging.hollywoodbets.net");
            var request = new RestRequest($"api/sports/{sportId}/countries", Method.GET);

            var response = client.Execute<SportCountries>(request);

            var countryList = new List<Countries>();

            var serializedSportObject = JsonConvert.DeserializeObject<SportCountries>(response.Content);

            foreach (var countries in serializedSportObject.responseObject)
            {
                countryList.Add(countries);

            }
            return countryList;
        }

    }
}
