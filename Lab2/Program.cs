using System;
using System.Linq;
using System.IO;
using Amazon.S3;
using Microsoft.Extensions.Configuration;
using System.Threading.Tasks;
using System.Threading;
using Amazon.S3.Model;
using System.Collections.Generic;

namespace Lab2
{    
    class Program
    {
        private static CancellationToken _token;
        private static readonly string InputBucketName = "vinlewt-qa-lab2-input";
        private static readonly string OuputBucketName = "vinlewt-qa-lab2-ouput";

        static void Main(string[] args)
        {
            var monitor = new CancellationTokenSource();
            _token = monitor.Token;

            Task.Factory.StartNew(Run);
            Console.WriteLine("Processing files");
            Console.ReadKey();            
            monitor.Cancel();
            monitor.Token.WaitHandle.WaitOne();
            Console.WriteLine("Stopped processing");                        
        }

        private static async Task Run()
        {
            var dataTransformer = new DataTransformer();
            var builder = new ConfigurationBuilder()
            .SetBasePath(Path.Combine(AppContext.BaseDirectory))
            .AddJsonFile("appsettings.json");        

            var config = builder.Build();
            var awsOptions = config.GetAWSOptions();     

            Console.WriteLine("Connecting to AWS");

            using(IAmazonS3 s3Client = awsOptions.CreateServiceClient<IAmazonS3>())
            {
                try
                {
                    var r = s3Client.ListBucketsAsync().Result;
                    Console.WriteLine($"You have {r.Buckets.Count} buckets");

                    //await s3Client.EnsureBucketExistsAsync(InputBucketName);
                    //await s3Client.EnsureBucketExistsAsync(OuputBucketName);                                
                }
                catch(Exception ex)
                {
                    Console.WriteLine(ex);
                }


                Console.WriteLine("Buckets created");

                while(_token.IsCancellationRequested == false)
                {
                    var inputFilesResponse = await s3Client.ListObjectsAsync(InputBucketName);
                    Console.WriteLine($"Found {inputFilesResponse.S3Objects.Count} objects to transform");

                    foreach(var inputFile in inputFilesResponse.S3Objects)
                    {
                        var s3Object = await s3Client.GetObjectAsync(inputFile.BucketName, inputFile.Key);
                        var contents = await s3Object.ContentAsString();                        
                        var transformedContent = dataTransformer.Transform(contents);

                        await s3Client.WriteContent(OuputBucketName, inputFile.Key, transformedContent);

                        Console.WriteLine("File transformed");

                        var presignRequest = new GetPreSignedUrlRequest
                        {
                            BucketName = OuputBucketName,
                            Key = inputFile.Key,
                            Expires = DateTime.Now.AddMinutes(15),
                            Protocol = Protocol.HTTP,
                            Verb = HttpVerb.GET
                        };

                        var presignedUrl =  s3Client.GetPreSignedURL(presignRequest);
                        Console.WriteLine(presignedUrl);
                    }

                    _token.WaitHandle.WaitOne(TimeSpan.FromSeconds(5));
                }                
            }
        }              
    }

    public class DataTransformer
    {
        public string Transform(string data)
        {
            return new string(data.Reverse().ToArray());
        }
    }


    public static class S3Extensions
    {
        public static async Task<string> ContentAsString(this GetObjectResponse s3)
        {
            using (Stream responseStream = s3.ResponseStream)
            using (StreamReader reader = new StreamReader(responseStream))
            {
                return await reader.ReadToEndAsync(); 
            }
        }

        public static async Task WriteContent(this IAmazonS3 s3, string bucketName, string key, string content, IDictionary<string, string> metaData = null)
        {
            var putRequest = new PutObjectRequest
            {
                BucketName = bucketName,
                Key = key,
                ContentBody = content
            };

            if(metaData != null)
            {
                foreach(var kvp in metaData)
                {
                    putRequest.Metadata.Add(kvp.Key, kvp.Value);
                }
            }

            await s3.PutObjectAsync(putRequest);
        }

        public static async Task CopyFileUp(this IAmazonS3 s3, string localPath, string bucketName, string key)
        {
            await s3.WriteContent(bucketName, key, File.ReadAllText(localPath));
        }

        public static async Task CopyFileDown(this IAmazonS3 s3, string localPath, string bucketName, string key)
        {
            var response = await s3.GetObjectAsync(bucketName, key);
            File.WriteAllText(localPath, await response.ContentAsString());
        }
    }
}
