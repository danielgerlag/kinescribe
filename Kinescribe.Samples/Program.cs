using System;
using System.IO;
using System.Threading.Tasks;
using Amazon;
using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using Amazon.Runtime;
using Kinescribe.Interface;
using Kinescribe.Services;

namespace Kinescribe.Samples
{
    class Program
    {
        static void Main(string[] args)
        {
            IStreamSubscriber subscriber = new StreamSubscriber();

            subscriber.Subscribe("my-app", "my-stream", record =>
            {
                using (var reader = new StreamReader(record.Data))
                {
                    Console.WriteLine($"Got event {record.SequenceNumber} - {reader.ReadToEnd()}");
                }
            });

            Publish(new EnvironmentVariablesAWSCredentials(), RegionEndpoint.USWest2, "my-stream", "hello world");

            Console.ReadLine();
        }

        static async Task Publish(AWSCredentials credentials, RegionEndpoint region, string streamName, string msg)
        {
            AmazonKinesisClient client = new AmazonKinesisClient(credentials, region);
            using (var stream = new MemoryStream())
            {
                var writer = new StreamWriter(stream);
                writer.Write(msg);
                writer.Flush();

                var response = await client.PutRecordAsync(new PutRecordRequest()
                {
                    StreamName = streamName,
                    PartitionKey = msg,
                    Data = stream
                });
            }
        }
    }
}
