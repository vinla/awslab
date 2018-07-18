using System;
using System.IO;
using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Microsoft.Extensions.Configuration;

namespace Test
{
    class Program
    {
        static void Main(string[] args)
        {
            var builder = new ConfigurationBuilder()
            .SetBasePath(Path.Combine(AppContext.BaseDirectory))
            .AddJsonFile("appsettings.json");        

            var config = builder.Build();
            var awsOptions = config.GetAWSOptions();

            using(IAmazonS3 s3Client = awsOptions.CreateServiceClient<IAmazonS3>())
            {
                var response = s3Client.ListBucketsAsync().Result;
                Console.WriteLine($"You have {response.Buckets.Count} buckets");
            }
        }
    }
}
