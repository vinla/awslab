using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.S3;
using Amazon.S3.Model;


// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace LambdaTransformer
{
    public class LambdaFunction
    {                
        public async Task FunctionHandler(S3Event s3Event, ILambdaContext context)
        {
            var srcBucket = s3Event.Records[0].S3.Bucket;
            var srcObject = s3Event.Records[0].S3.Object;

            using(var s3Service = new AmazonS3Client() )
            {
                var incoming = await s3Service.GetObjectAsync(srcBucket.Name, srcObject.Key);

                using(var streamReader = new StreamReader(incoming.ResponseStream) )
                {
                    var content = streamReader.ReadToEnd();
                    var payload = new string(content.Reverse().ToArray());

                    var putRequest = new PutObjectRequest
                    {
                        BucketName = "vinlewt-tx-output",
                        Key = srcObject.Key,
                        ContentBody = payload
                    };

                    await s3Service.PutObjectAsync(putRequest);
                }
            }            
        }
    }
}
