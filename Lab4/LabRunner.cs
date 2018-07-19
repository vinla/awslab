using System;
using System.Linq;
using System.IO;
using Amazon.DynamoDBv2;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.Json;
using System.Threading.Tasks;
using System.Threading;
using System.Collections.Generic;
using Amazon.SimpleNotificationService;
using Amazon.SQS;
using Amazon.SQS.Model;
using Newtonsoft.Json;

namespace Lab4
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

            using(var sns = awsOptions.CreateServiceClient<IAmazonSimpleNotificationService>())
            {
                // var emailNotificationPublisher = new SnsPublisher(sns, "arn:aws:sns:eu-west-1:958643880814:EmailSNSTopic");
                // await emailNotificationPublisher.PublishMessage("Hello world");

                // var dataPusher = new SnsPublisher(sns, config["OrderSNS"]);
                // await dataPusher.PublishJson(new SimpleMessage
                // {
                //     Id = 54321,
                //     Subject = "This is a secret",
                //     IsSuperDooperImportant = false
                // });                
            }

            using(var sqsService = awsOptions.CreateServiceClient<IAmazonSQS>())
            {
                var queueReader = new SqsReader(sqsService, "MySQSQueue_A");
                var messages = Enumerable.Empty<SimpleMessage>();
                
                foreach(var message in await queueReader.ReadJson<SimpleMessage>())
                {
                    Console.WriteLine(message.Id);
                    Console.WriteLine(message.Subject);
                }                                    
            }
        }
    }  

    public class SimpleMessage
    {
        public int Id { get ; set; }
        public string Subject { get; set; }
        public bool IsSuperDooperImportant { get; set; }
    }

    public class SnsPublisher
    {
        private readonly string _topicArn;
        private readonly IAmazonSimpleNotificationService _snsService;
        public SnsPublisher(IAmazonSimpleNotificationService snsService, string topicArn)
        {
            _snsService = snsService;
            _topicArn = topicArn;
        }

        public async Task PublishMessage(string message)
        {
            await _snsService.PublishAsync(_topicArn, message);
        }

        public async Task PublishJson(object data)
        {
            await PublishMessage(JsonConvert.SerializeObject(data));
        }
    }

    public class SqsReader
    {
        private readonly IAmazonSQS _sqsService;
        private readonly string _queueName;
        private string _queueUrl;
        public SqsReader(IAmazonSQS sqsService, string queueName)
        {
            _sqsService = sqsService;            
            _queueName = queueName;            
        }

        public async Task<IEnumerable<TData>> ReadJson<TData>()
        {
            var results = new List<TData>();

            if(String.IsNullOrEmpty(_queueUrl))
                _queueUrl = (await _sqsService.GetQueueUrlAsync(_queueName)).QueueUrl;
            
            Console.WriteLine(_queueUrl);

            var messageRequest = new ReceiveMessageRequest
            {
                MaxNumberOfMessages = 10,
                QueueUrl = _queueUrl,
                WaitTimeSeconds = 5
            };

            var response = await _sqsService.ReceiveMessageAsync(messageRequest);            
            Console.WriteLine(">" + response.Messages.Count);   

            foreach(var message in response.Messages)
            {
                try
                {
                    results.Add(JsonConvert.DeserializeObject<TData>(message.Body));
                    await _sqsService.DeleteMessageAsync(_queueUrl, message.ReceiptHandle);
                }
                catch(Exception)
                {}
            }

            return results;
        }
    }

}