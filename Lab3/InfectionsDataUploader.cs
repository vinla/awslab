using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.S3;

namespace Lab3
{
    public class InfectionsDataUploader
    {
        private readonly IAmazonS3 _s3Service;
        private readonly IAmazonDynamoDB _dbService;

        public InfectionsDataUploader(IAmazonS3 s3Service, IAmazonDynamoDB dbService)
        {
            _s3Service = s3Service;
            _dbService = dbService;
        }

        public async Task UploadData()
        {
            using(var s3Response = await _s3Service.GetObjectAsync("us-west-2-aws-staging", "awsu-ilt/AWS-100-DEV/v2.2/binaries/input/lab-3-dynamoDB/InfectionsData.csv"))
            {
                using(var reader = new StreamReader(s3Response.ResponseStream) )
                {
                    var line = reader.ReadLine();

                    while((line = reader.ReadLine()) != null)
                    {
                        var data = line.Split(",");
                        try
                        {
                            if (!data[0].ToLower().Equals("patientid"))
                            {                               
                                var requestDiseaseListing = new PutItemRequest
                                {
                                    TableName = Constants.TableName,
                                    Item = new Dictionary<string, AttributeValue>()
                                    {
                                        { Constants.PatientId, new AttributeValue(data[0]) },
                                        { Constants.City,      new AttributeValue(data[1]) },
                                        { Constants.Date,      new AttributeValue(data[2]) },
                                    }
                                };            
                                await _dbService.PutItemAsync(requestDiseaseListing);
                                Console.WriteLine("Added item:" + line);
                            }
                        }
                        catch (AmazonDynamoDBException ex)
                        {
                            Console.WriteLine("Failed to create item");
                            Console.WriteLine(ex.Message);    
                            Console.WriteLine(ex.InnerException.Message);                                
                        }
                    }
                }
            }
        }        
    }
}