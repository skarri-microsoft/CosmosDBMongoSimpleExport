namespace CosmosMongoDBReadWriteSample
{
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.IO;
    using System.Linq;
    using System.Threading.Tasks;
    using MongoDB.Bson;
    using MongoDB.Driver;

    class Program
    {

        private static MongoClient srcMongoClient;
        private static string srcDbName = ConfigurationManager.AppSettings["srcDbName"];
        private static string srcCollectionName = ConfigurationManager.AppSettings["srcCollectionName"];
        private static IMongoDatabase srcDatabase;
        private static IMongoCollection<BsonDocument> srcDocStoreCollection;

        private static MongoClient destMongoClient;
        private static string destDbName = ConfigurationManager.AppSettings["destDbName"];
        private static string destCollectionName = ConfigurationManager.AppSettings["destCollectionName"];
        private static IMongoDatabase destDatabase;
        private static IMongoCollection<BsonDocument> destDocStoreCollection;

        private static List<BsonDocument> insertFailedDocs = new List<BsonDocument>();

        private static int insertRetries = Int32.Parse(ConfigurationManager.AppSettings["insertRetries"]);
        private static int minWait = 1500;
        private static int maxWait = 3000;
        private static long docsCount = 0;


        static void Main(string[] args)
        {

            string srcConnectionString =
               ConfigurationManager.AppSettings["src-conn"];
            MongoClientSettings srcSettings = MongoClientSettings.FromUrl(
                new MongoUrl(srcConnectionString)
            );
            srcMongoClient = new MongoClient(srcSettings);
            srcDatabase = srcMongoClient.GetDatabase(srcDbName);
            srcDocStoreCollection = srcDatabase.GetCollection<BsonDocument>(srcCollectionName);

            string destConnectionString =
                ConfigurationManager.AppSettings["dest-conn"];
            MongoClientSettings destSettings = MongoClientSettings.FromUrl(
                new MongoUrl(destConnectionString)
            );
            destMongoClient = new MongoClient(destSettings);
            destDatabase = destMongoClient.GetDatabase(destDbName);
            destDocStoreCollection = destDatabase.GetCollection<BsonDocument>(destCollectionName);

            ExportDocuments().Wait();

            if (insertFailedDocs.Any())
            {
                using (var sw = new StreamWriter(ConfigurationManager.AppSettings["failedDocsPath"]))
                {
                    foreach (var doc in insertFailedDocs)
                    {
                        sw.WriteLine(doc.ToJson());
                    }
                }

                Console.WriteLine("Not all documents were exported, failed documents located @: {0}", ConfigurationManager.AppSettings["failedDocsPath"]);
            }

            Console.WriteLine("Press enter to exit...");

            Console.ReadLine();
        }


        private static async Task InsertAllDocuments(IEnumerable<BsonDocument> docs)
        {
            var tasks = new List<Task>();
            for (int j = 0; j < docs.Count(); j++)
            {
                tasks.Add(InsertDocument(docs.ToList()[j]));
            }

            await Task.WhenAll(tasks).ConfigureAwait(false);
            docsCount = docsCount + docs.Count();
            Console.WriteLine("Total documents copied so far: {0}",docsCount);
        }

        private static async Task ExportDocuments()
        {
            FilterDefinition<BsonDocument> filter = FilterDefinition<BsonDocument>.Empty;
            FindOptions<BsonDocument> options = new FindOptions<BsonDocument>
            {
                BatchSize = Int32.Parse(ConfigurationManager.AppSettings["batchsize"]),
                NoCursorTimeout = false
            };
            using (IAsyncCursor<BsonDocument> cursor = await srcDocStoreCollection.FindAsync(filter,options))
            {
                bool isSucceed = false;

                while (!isSucceed)
                {

                    try
                    {
                        while (await cursor.MoveNextAsync())
                        {
                            IEnumerable<BsonDocument> batch = cursor.Current;
                            await InsertAllDocuments(batch);
                        }
                    }
                    catch (Exception ex)
                    {

                        if (!IsThrottled(ex))
                        {
                            Console.WriteLine("ERROR: With collection {0}", ex.ToString());
                            throw;
                        }
                        else
                        {
                            // Thread will wait in between 1.5 secs and 3 secs.
                            System.Threading.Thread.Sleep(new Random().Next(minWait, maxWait));
                        }
                    }

                    isSucceed = true;
                    break;
                }
            }

        }


        private static async Task InsertDocument(BsonDocument doc)
        {
            bool isSucceed = false;
            for (int i = 0; i < insertRetries; i++)
            {
                try
                {
                    await destDocStoreCollection.InsertOneAsync(doc);

                    isSucceed = true;
                    //Operation succeed just break the loop
                    break;
                }
                catch (Exception ex)
                {

                    if (!IsThrottled(ex))
                    {
                        Console.WriteLine("ERROR: With collection {0}", ex.ToString());
                        throw;
                    }
                    else
                    {
                        // Thread will wait in between 1.5 secs and 3 secs.
                        System.Threading.Thread.Sleep(new Random().Next(minWait, maxWait));
                    }
                }
            }

            if (!isSucceed)
            {
                insertFailedDocs.Add(doc);
            }

        }

        private static bool IsThrottled(Exception ex)
        {
            return ex.Message.ToLower().Contains("Request rate is large".ToLower());
        }
    }
}
