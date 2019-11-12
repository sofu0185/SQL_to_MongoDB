using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace SQL_to_MongoDB
{
    class Program
    {
        static MongoClient client = new MongoClient("mongodb://127.0.0.1:27017");

        static async Task Main(string[] args)
        {
            /*
             {
                Timestamp: 0000-00-00,
                Result: 0,
                Station: "",
                Stof: ""
             }
             */
            IEnumerable<MongoData> data = await GetAllMeasurementsFromSqlAsync();

            await InsertDataIntoMongoAsync(data);

            await GetAndPrintAsync();

            Console.ReadKey();
        }

        public static async Task<IEnumerable<MongoData>> GetAllMeasurementsFromSqlAsync()
        {
            List<MongoData> data = new List<MongoData>();

            using (SqlConnection cn = new SqlConnection("Server=LAPTOP-IO4O0116;Database=AirQuality;Integrated Security=SSPI;"))
            {
                cn.Open();
                using (SqlCommand cmd = new SqlCommand("SELECT * FROM MeasurementsData", cn))
                {
                    SqlDataReader reader = await cmd.ExecuteReaderAsync();

                    while (await reader.ReadAsync())
                    {
                        data.Add(new MongoData
                        {
                            Timestamp = reader.GetDateTime(0),
                            Result = reader.GetDouble(1),
                            Station = reader.GetString(2),
                            Stof = reader.GetString(3)
                        });
                    }
                }
            }
            Console.WriteLine($"Loaded all measurements from sql database. Total: {data.Count()}");

            return data;
        }

        public static async Task InsertDataIntoMongoAsync(IEnumerable<MongoData> data)
        {
            IMongoDatabase db = client.GetDatabase("AirQuality");

            string collectionName = "measurements";
            IMongoCollection<MongoData> measurements = db.GetCollection<MongoData>(collectionName);
            await db.DropCollectionAsync(collectionName);
            await db.CreateCollectionAsync(collectionName);

            measurements.InsertMany(data);

            long count = await measurements.EstimatedDocumentCountAsync();
            Console.WriteLine($"MongoDB measurement count: {count}");

            
        }
        public static async Task GetAndPrintAsync()
        {
            IMongoCollection<MongoData> collection = client.GetDatabase("AirQuality").GetCollection<MongoData>("measurements");

            Console.WriteLine();

            IFindFluent<MongoData, MongoData> cursor = collection.Find(x => x.Station.Contains("NORD2") && x.Stof == "Ozon" && x.Timestamp <= new DateTime(2018, 01, 31));
            await cursor.Sort("{Timestamp: -1}").ForEachAsync(x => Console.WriteLine(x));
        }
    }



    class MongoData
    {
        [BsonId]
        public ObjectId Id { get; set; }
        [BsonElement("Timestamp")]
        public DateTime Timestamp { get; set; }
        [BsonElement("Result")]
        public double Result { get; set; }
        [BsonElement("Station")]
        public string Station { get; set; }
        [BsonElement("Stof")]
        public string Stof { get; set; }

        public override string ToString()
        {
            return '{' + $" Id: {Id}, Timestamp: {Timestamp}, Result: {Result}, Station: {Station}, Stof: {Stof} " + '}';
        }
    }
}
