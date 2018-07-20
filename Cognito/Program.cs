using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Amazon.Extensions.NETCore.Setup;
using Amazon.CognitoIdentityProvider;
using Amazon.CognitoIdentityProvider.Model;
using Amazon.SecurityToken;
using Amazon.SecurityToken.Model;
using Amazon;

namespace Cognito
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var awsOptions = new AWSOptions();            

            using(var cognito = new AmazonCognitoIdentityProviderClient(RegionEndpoint.EUWest1))
            {
                var initAuthRequest = new AdminInitiateAuthRequest
                {                    
                    AuthFlow = AuthFlowType.ADMIN_NO_SRP_AUTH,
                    AuthParameters = new Dictionary<string, string>
                    {

                    },
                    UserPoolId = "eu-west-1_BJ8QvSs1g",
                    ClientId = "5ive4k9rhvvo776p7rppa5gcd5"                    
                };

                var response = await cognito.AdminInitiateAuthAsync(initAuthRequest);                

                var challengeResponse = new AdminRespondToAuthChallengeRequest
                {
                    ChallengeName = response.ChallengeName,                                        
                    Session = response.Session,
                    ClientId = "Filer",                                        
                    UserPoolId = "Services"
                };

                var authResponse = await cognito.AdminRespondToAuthChallengeAsync(challengeResponse);

                using(var securityTokenProvider = new AmazonSecurityTokenServiceClient())
                {
                    var assumeRoleRequest = new AssumeRoleWithWebIdentityRequest
                    {
                        RoleArn = "",
                        
                        WebIdentityToken = authResponse.AuthenticationResult.AccessToken
                    };

                    var roleCreds = await securityTokenProvider.AssumeRoleWithWebIdentityAsync(assumeRoleRequest);

                    awsOptions.Credentials = roleCreds.Credentials;
                }            
            }            
        }    
    }

    public class CognitoIdentityProviderSettings
    {
        public CognitoIdentityProviderSettings()
        {
            this.Region = RegionEndpoint.USEast1;
        }
        public RegionEndpoint Region { get; set; }
        public string IdentityPoolID { get; set; }
        public string UserPoolID { get; set; }
        public string AppClientID { get; set; }
        public string IdentityProviderName
        {
            get
            {
                return string.Format("cognito-idp.{0}.amazonaws.com/{1}", this.Region.SystemName, this.UserPoolID);
            }
        }
    }
}
