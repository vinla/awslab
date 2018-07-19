using System;
using System.Linq;
using System.IO;
using Amazon.DynamoDBv2;
using Microsoft.Extensions.Configuration;
using System.Threading.Tasks;
using System.Threading;
using System.Collections.Generic;
using Amazon.DynamoDBv2.Model;
using Amazon.S3;
using Amazon.S3.Model;

namespace Lab3
{
    public class LabRunner
    {
        public async Task Run()
        {            
            var builder = new ConfigurationBuilder()
            .SetBasePath(Path.Combine(AppContext.BaseDirectory))
            .AddJsonFile("appsettings.json");        

            var config = builder.Build();
            var awsOptions = config.GetAWSOptions();  

            using(IAmazonDynamoDB dbService = awsOptions.CreateServiceClient<IAmazonDynamoDB>())
            {
                // await RemoveInfectionsTableIfExists(dbService);
                // await CreateTable(dbService);

                await QueryByCity(dbService, "Reno");
            }
        }

        private async Task CreateTable(IAmazonDynamoDB dbService)
        {
            var createTableRequest = new CreateTableRequest
            {
                TableName = Constants.TableName,
                AttributeDefinitions = new List<AttributeDefinition>
                {
                    new AttributeDefinition
                    {
                        AttributeName = "PatientId",
                        AttributeType = "S"
                    },
                    new AttributeDefinition
                    {
                        AttributeName = "City",
                        AttributeType = "S"
                    },
                    new AttributeDefinition
                    {
                        AttributeName = "Date",
                        AttributeType = "S"
                    }
                },
                KeySchema = new List<KeySchemaElement>
                {
                    new KeySchemaElement
                    {
                        AttributeName = "PatientId",
                        KeyType = "HASH"
                    }
                },
                ProvisionedThroughput = new ProvisionedThroughput
                {
                    ReadCapacityUnits = 5L,
                    WriteCapacityUnits = 10L
                },
                GlobalSecondaryIndexes = { new GlobalSecondaryIndex
                {
                    IndexName = Constants.CityDateIndexName,
                    ProvisionedThroughput = new ProvisionedThroughput
                    {
                        ReadCapacityUnits = 5L,
                        WriteCapacityUnits = 5L
                    },
                    Projection = new Projection { ProjectionType = "ALL" },
                    KeySchema = new List<KeySchemaElement>
                    {
                        new KeySchemaElement
                        {
                            AttributeName = Constants.City, 
                            KeyType = "HASH"
                        },
                        new KeySchemaElement
                        {
                            AttributeName = Constants.Date, 
                            KeyType = "RANGE"
                        }
                    }
                }}
            };
            
            var response = await dbService.CreateTableAsync(createTableRequest);
            var created = false;

            while(created == false)
            {
                try
                {
                    await dbService.DescribeTableAsync(Constants.TableName);
                    created = true;
                }
                catch(AmazonDynamoDBException)
                {
                    Thread.Sleep(TimeSpan.FromMilliseconds(5000));
                }
            }

            Console.WriteLine("Table created");
        }

        private async Task RemoveInfectionsTableIfExists(IAmazonDynamoDB dbService)
        {
            try
            {
                string tableName = Constants.TableName;                

                var request = new DeleteTableRequest { TableName = tableName };
                await dbService.DeleteTableAsync(request);
            }
            catch (ResourceNotFoundException)
            {                
            }
        }

        private static async Task QueryByCity(IAmazonDynamoDB dbService, string city)
        {
            QueryRequest request = new QueryRequest
            {
                TableName = Constants.TableName,
                IndexName = Constants.CityDateIndexName,
                KeyConditionExpression = "City = :v_city",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    {":v_city", new AttributeValue(city)}
                }
            };

            var results = await dbService.QueryAsync(request);            
            Console.WriteLine("Rows found = " + results.Count);
            foreach(var result in results.Items)
            {
                Console.WriteLine($"{result["PatientId"].S} - {result["City"].S} - {result["Date"].S}");
            }
        }
    }
    
    public static class Constants
    {
        public static readonly string CityDateIndexName = "InfectionsByCityDate";

        public static readonly string TableName = "Infections";
        public static readonly string PatientId = nameof(PatientId);

        public static readonly string City = nameof(City);

        public static readonly string Date = nameof(Date);
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