using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Text;

namespace UtilitiesLibrary
{
    public class RedisConnection
    {
        private static string _redisServerIp = "";

        public RedisConnection(string redisServerIp)
        {
            _redisServerIp = redisServerIp;
        }

        private static Lazy<ConnectionMultiplexer> lazyConnection = new Lazy<ConnectionMultiplexer>(() =>
        {
            return ConnectionMultiplexer.Connect($"{_redisServerIp},abortConnect=false");
        });

        public static ConnectionMultiplexer Connection
        {
            get
            {
                return lazyConnection.Value;
            }
        }

        public void SaveKeyValueToDB(string key, string value, int redisDbNo, long expiryTimeInMinutes)
        {
            var redisDb = Connection.GetDatabase(redisDbNo);
            redisDb.StringSet(key, value);
            redisDb.KeyExpire(key, DateTime.Now.AddMinutes(expiryTimeInMinutes));
        }

        public string GetValueFromKey(string key, int redisDBNo)
        {
            var redisDb = Connection.GetDatabase(redisDBNo);
            return redisDb.StringGet(key);
        }

        public bool DeleteKeyValue(string key, int redisDBNo)
        {
            var redisDb = Connection.GetDatabase(redisDBNo);
            return redisDb.KeyDelete(key);
        }

        public bool SetExpiry(string key, DateTime expiryTime, int redisDBNo)
        {
            var redisDb = Connection.GetDatabase(redisDBNo);
            return redisDb.KeyExpire(key, expiryTime);
        }
    }
}
