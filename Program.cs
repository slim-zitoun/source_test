using Elasticsearch.Net;
using Nest;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Reindexation
{




    public static class exep
    {
        public static T ThrowOnError<T>(this T response, string actionDescription = null) where T : IResponse
        {
            if (!response.IsValid)
            {
                throw new Exception(actionDescription == null ? string.Empty : "Failed to " + actionDescription + ": " + response.ServerError.Error);
            }

            return response;
        }

    }


    public class IndexMapping
    {
        public string indexFrom { get; set; }
        public string indexTo { get; set; }
    }

    public class Configuration
    {
        public string connectionString { get; set; }
        public int size { get; set; }
        public List<IndexMapping> indexMappings { get; set; }
    }



    class Program
    {

        static ConcurrentDictionary<int, IEnumerable<IHit<object>>> DataStores;
        static bool IndexinProgress;
        static int ActiveProcess;
        static int TotalSend;


        static void Main(string[] args)
        {
           


            Configuration config = new Configuration();
            using (StreamReader r = new StreamReader("config.json"))
            {
                string json = r.ReadToEnd();
                config = JsonConvert.DeserializeObject<Configuration>(json);
            }

            /// string connectionString = "http://kmpirels101.tnsad.com:9200/";

            ConnectionSettings _settings = new ConnectionSettings(new Uri(config.connectionString))
                                                         .EnableTrace(false)
                                                         .SetDefaultIndex(".clips-all")
                                                         .UsePrettyResponses()
                                                         .SetTimeout(2000000);




            ElasticClient client = new ElasticClient(_settings);

            Stopwatch stopWatch = new Stopwatch();
            stopWatch.Start();

            config.indexMappings.ForEach(e => IndexData(client, e.indexFrom, e.indexTo, config.size));

            TimeSpan ts = stopWatch.Elapsed;

            // Format and display the TimeSpan value.
            string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}",
                ts.Hours, ts.Minutes, ts.Seconds,
                ts.Milliseconds / 10);

            Console.WriteLine("..................................................");
            Console.WriteLine("RunTime " + elapsedTime);


            Console.ReadLine();

            // TODO: Don't forget to delete the old index if you want
        }

        private static void IndexData(ElasticClient client, string indexFrom, string indexTo, int size)
        {
            IndexinProgress = true;
            DataStores = new ConcurrentDictionary<int, IEnumerable<IHit<object>>>();
            TotalSend = 0;

             new Task(()=> DoReindex(client, indexTo, size)).Start();
           
            Console.WriteLine("..................................................");
            Console.WriteLine("Reindexing " + indexFrom);
            var searchResult = client.Search<object>(s => s.Index(indexFrom).AllTypes().From(0).Size(size).Query(q => q.MatchAll()).SearchType(SearchType.Scan).Scroll("10m"));
            if (searchResult.Total <= 0)
            {
                Console.WriteLine("Existing index has no documents, nothing to reindex.");
            }
            else
            {
                var page = 0;
                //  IBulkResponse bulkResponse = null;
                do
                {
                    var result = searchResult;
                    searchResult = client.Scroll<object>(s => s.Scroll("20m").ScrollId(result.ScrollId));
                    if (searchResult.Documents != null && searchResult.Documents.Any())
                    {
                        searchResult.ThrowOnError("reindex scroll " + page);
                        // StartReindex(client, indexTo, size, searchResult, page);

                        DataStores[page] = searchResult.Hits;
                    }

                    ++page;
                }
                while (searchResult.IsValid && searchResult.Documents != null && searchResult.Documents.Any());

                IndexinProgress = false;

                Console.WriteLine("Reindexing complete!");
            }

        }

        private static Task StartReindex(ElasticClient client, string indexTo, int size)
        {

            Task task = new Task(() =>
            {
                Interlocked.Add(ref ActiveProcess, 1);
               
                var page  = DataStores.First().Key;
                IEnumerable<IHit<object>> data;


                if (DataStores.TryRemove(page, out data))
                {
                    var bulkResponse = client.Bulk(b =>
                        {
                            foreach (var hit in data)
                            {
                                b.Index<object>(bi => bi.Document(hit.Source).Type(hit.Type).Index(indexTo).Id(hit.Id));
                            }

                            return b;
                        });

                    Interlocked.Add(ref ActiveProcess, -1);

                    if (bulkResponse.IsValid)
                    {
                        Interlocked.Add(ref TotalSend, size);
                        Console.WriteLine("Progress ... page: " + page + " ... Total: " + TotalSend );
                    }
                    else
                    {
                        DataStores[page] = data;
                        Console.WriteLine("Erreur ... page: " + page);
                    }
                }
            });

            return task;
        }

        private static void DoReindex(ElasticClient client, string indexTo, int size)
    {
        
                if (ActiveProcess < 30 && DataStores.Count > 0)
                {
                    StartReindex(client, indexTo, size).Start();
                }

                if (DataStores.Count > 0 || IndexinProgress)
                {
                     Thread.Sleep(500);
                     DoReindex(client, indexTo, size); 
                }
    }
    
    }
}
