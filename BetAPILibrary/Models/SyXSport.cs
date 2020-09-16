using System;
using System.Collections.Generic;
using System.Text;

namespace BetAPILibrary.Models
{
    public class Sport
    {
        public List<SyXSport> responseObject { get; set; }
        public string responseMessage { get; set; }
        public int responseType { get; set; }
    }
    public class SyXSport
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public int SportTypeId { get; set; }
        public string SportIcon { get; set; }
    }

    public class SportCountries
    {
        public List<Countries> responseObject { get; set; }
        public string responseMessage { get; set; }
        public int responseType { get; set; }
    }


}
