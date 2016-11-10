using System;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Rest;
using Microsoft.Threading;
using ApiHost.Models;
using System.IO;
using System.Threading;
using Microsoft.Rest.Serialization;
using System.Net.Http.Headers;
using System.Configuration;
using System.Net;
using System.Collections.Generic;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.PowerBI.Api.V1;
using Microsoft.PowerBI.Api.V1.Models;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Linq; 

namespace ProvisionSample
{
    class Program
    {
        const string version = "?api-version=2016-01-29";
        const string armResource = "https://management.core.windows.net/";
        static string clientId = "ea0616ba-638b-4df5-95b9-636659ae5121";
        static Uri redirectUri = new Uri("urn:ietf:wg:oauth:2.0:oob");

        static string apiEndpointUri = ConfigurationManager.AppSettings["powerBiApiEndpoint"];
        static string azureEndpointUri = ConfigurationManager.AppSettings["azureApiEndpoint"];
        static string subscriptionId = ConfigurationManager.AppSettings["subscriptionId"];
        static string resourceGroup = ConfigurationManager.AppSettings["resourceGroup"];
        static string workspaceCollectionName = ConfigurationManager.AppSettings["workspaceCollectionName"];
        static string username = ConfigurationManager.AppSettings["username"];
        static string password = ConfigurationManager.AppSettings["password"];
        static string accessKey = ConfigurationManager.AppSettings["accessKey"];
        static string azureToken = null;
        static string[] workspaceID;
        static string[] Customers;

        static WorkspaceCollectionKeys accessKeys = null;
        //HP: 2 customers
        const int customersNumber = 2;
       
        //Sync VS

        static void Main(string[] args)
        {
            
            Customers = new string[customersNumber];
            workspaceID = new string[customersNumber];

            for (int i = 0; i < customersNumber; i++)
            {
                Customers[i] = $"Customer{i}";
            }


            if (!string.IsNullOrWhiteSpace(accessKey))
            {
                accessKeys = new WorkspaceCollectionKeys
                {
                    Key1 = accessKey
                };
            }

            AsyncPump.Run(async delegate
            {
                await Run();
            });

            Console.ReadKey(true);
        }

        static async Task Run()
        {
            Console.ResetColor();
            var exit = false;

            try
            {
                Console.ForegroundColor = ConsoleColor.Yellow;

                Console.WriteLine("This application does the following operations:");
                Console.WriteLine("1. Provision of a new workspace collection in the specified Azure subscription");               
                Console.WriteLine("2. Retrieve the workspace collection's API key");               
                Console.WriteLine("3. Provision a new workspace for each customer in the workspace collection");
                Console.WriteLine("4. Import PBIX Desktop file into every workspace created - one foreach customer");
                Console.WriteLine("5. Update connection string info for the dataset of the first workspace created");              
                Console.WriteLine();


                Console.WriteLine("1. Provision of a new workspace collection in the specified Azure subscription");
                Console.ForegroundColor = ConsoleColor.White;


                Console.Write("Azure Subscription ID:");
                subscriptionId = Console.ReadLine();
                //Console.WriteLine();
                Console.Write("Azure Resource Group:");
                resourceGroup = Console.ReadLine();
                //Console.WriteLine();
                Console.Write("Workspace Collection Name:");
                workspaceCollectionName = Console.ReadLine();
                await CreateWorkspaceCollection(subscriptionId, resourceGroup, workspaceCollectionName);
                accessKeys = await ListWorkspaceCollectionKeys(subscriptionId, resourceGroup, workspaceCollectionName);

                Console.ForegroundColor = ConsoleColor.Cyan;
                Console.WriteLine("Workspace collection created successfully");
                Console.ForegroundColor = ConsoleColor.Yellow;

                Console.WriteLine("2. Retrieve the workspace collection's API key");

                accessKeys = await ListWorkspaceCollectionKeys(subscriptionId, resourceGroup, workspaceCollectionName);
                Console.ForegroundColor = ConsoleColor.Cyan;
                Console.WriteLine("Key1: {0}", accessKeys.Key1);
                Console.ForegroundColor = ConsoleColor.Yellow;

                Console.WriteLine("3. Provision a new workspace for each customer in the workspace collection");

                //one workspace for each customer
                for (int i=0; i<Customers.Length; i++)
                {
                    var workspace = await CreateWorkspace(workspaceCollectionName);
                    workspaceID[i] = workspace.WorkspaceId;
                    Console.ForegroundColor = ConsoleColor.Cyan;
                    Console.WriteLine($"WorkspaceID: {workspaceID[i]}");
                    
                }
                Console.ForegroundColor = ConsoleColor.Yellow;

                Console.WriteLine("4. Import PBIX Desktop file into every existing workspaces");

                Console.ForegroundColor = ConsoleColor.White;
                Console.Write("Dataset Name:");
                var datasetName = Console.ReadLine();
                Console.Write("File Folder:");
                var filePath = Console.ReadLine();
                Console.Write("File Name:");
                var fileName = Console.ReadLine();
                var oldPath = filePath + fileName + ".pbix";

                for (int i=0; i<Customers.Length; i++)
                {                    
                    var newFileName = fileName + "_" + Customers[i] + ".pbix";
                    var newPath = filePath + newFileName;
                    System.IO.File.Copy(oldPath, newPath);
                    var import = await ImportPbix(workspaceCollectionName, workspaceID[i], datasetName, newPath);
                    
                }

                Console.ForegroundColor = ConsoleColor.Yellow;

                Console.WriteLine("5. Update connection string info for the dataset of the first workspace created");
                Console.ForegroundColor = ConsoleColor.White;                
                await UpdateConnection(workspaceCollectionName, workspaceID[0]);
                Console.ForegroundColor = ConsoleColor.Cyan;
                Console.WriteLine("Connection information updated successfully.");          
                Console.WriteLine("PROCESS COMPLETED");
                Console.ReadLine();

                //await Run();

     
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Ooops, something broke: {0}", ex);
                Console.WriteLine();
            }

            if (!exit)
            {
                await Run();
            }
        }

      

        /// <summary>
        /// Creates a new Power BI Embedded workspace collection
        /// </summary>
        /// <param name="subscriptionId">The azure subscription id</param>
        /// <param name="resourceGroup">The azure resource group</param>
        /// <param name="workspaceCollectionName">The Power BI workspace collection name to create</param>
        /// <returns></returns>
        static async Task CreateWorkspaceCollection(string subscriptionId, string resourceGroup, string workspaceCollectionName)
        {
            var url = string.Format("{0}/subscriptions/{1}/resourceGroups/{2}/providers/Microsoft.PowerBI/workspaceCollections/{3}{4}", azureEndpointUri, subscriptionId, resourceGroup, workspaceCollectionName, version);

            HttpClient client = new HttpClient();

            using (client)
            {
                var content = new StringContent(@"{
                                                ""location"": ""southcentralus"",
                                                ""tags"": {},
                                                ""sku"": {
                                                    ""name"": ""S1"",
                                                    ""tier"": ""Standard""
                                                }
                                            }", Encoding.UTF8);
                content.Headers.ContentType = MediaTypeHeaderValue.Parse("application/json; charset=utf-8");

                var request = new HttpRequestMessage(HttpMethod.Put, url);
                // Set authorization header from you acquired Azure AD token
                request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", await GetAzureAccessTokenAsync());
                request.Content = content;

                var response = await client.SendAsync(request);
                if (response.StatusCode != HttpStatusCode.OK)
                {
                    var responseText = await response.Content.ReadAsStringAsync();
                    var message = string.Format("Status: {0}, Reason: {1}, Message: {2}", response.StatusCode, response.ReasonPhrase, responseText);
                    throw new Exception(message);
                }

                var json = await response.Content.ReadAsStringAsync();
                return;
            }
        }

        /// <summary>
        /// Gets the workspace collection access keys for the specified collection
        /// </summary>
        /// <param name="subscriptionId">The azure subscription id</param>
        /// <param name="resourceGroup">The azure resource group</param>
        /// <param name="workspaceCollectionName">The Power BI workspace collection name</param>
        /// <returns></returns>
        static async Task<WorkspaceCollectionKeys> ListWorkspaceCollectionKeys(string subscriptionId, string resourceGroup, string workspaceCollectionName)
        {
            var url = string.Format("{0}/subscriptions/{1}/resourceGroups/{2}/providers/Microsoft.PowerBI/workspaceCollections/{3}/listkeys{4}", azureEndpointUri, subscriptionId, resourceGroup, workspaceCollectionName, version);

            HttpClient client = new HttpClient();

            using (client)
            {
                var request = new HttpRequestMessage(HttpMethod.Post, url);
                // Set authorization header from you acquired Azure AD token
                request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", await GetAzureAccessTokenAsync());
                request.Content = new StringContent(string.Empty);
                var response = await client.SendAsync(request);

                if (response.StatusCode != HttpStatusCode.OK)
                {
                    var responseText = await response.Content.ReadAsStringAsync();
                    var message = string.Format("Status: {0}, Reason: {1}, Message: {2}", response.StatusCode, response.ReasonPhrase, responseText);
                    throw new Exception(message);
                }

                var json = await response.Content.ReadAsStringAsync();
                return SafeJsonConvert.DeserializeObject<WorkspaceCollectionKeys>(json);
            }
        }

        
        /// <summary>
        /// Creates a new Power BI Embedded workspace within the specified collection
        /// </summary>
        /// <param name="workspaceCollectionName">The Power BI workspace collection name</param>
        /// <returns></returns>
        static async Task<Workspace> CreateWorkspace(string workspaceCollectionName)
        {
            using (var client = await CreateClient())
            {
                // Create a new workspace witin the specified collection
                return await client.Workspaces.PostWorkspaceAsync(workspaceCollectionName);
            }
        }

       

        /// <summary>
        /// Imports a Power BI Desktop file (pbix) into the Power BI Embedded service
        /// </summary>
        /// <param name="workspaceCollectionName">The Power BI workspace collection name</param>
        /// <param name="workspaceId">The target Power BI workspace id</param>
        /// <param name="datasetName">The dataset name to apply to the uploaded dataset</param>
        /// <param name="filePath">A local file path on your computer</param>
        /// <returns></returns>
        static async Task<Import> ImportPbix(string workspaceCollectionName, string workspaceId, string datasetName, string filePath)
        {
            using (var fileStream = File.OpenRead(filePath))
            {
                using (var client = await CreateClient())
                {
                    // Set request timeout to support uploading large PBIX files
                    client.HttpClient.Timeout = TimeSpan.FromMinutes(60);
                    client.HttpClient.DefaultRequestHeaders.Add("ActivityId", Guid.NewGuid().ToString());

                    // Import PBIX file from the file stream
                    var import = await client.Imports.PostImportWithFileAsync(workspaceCollectionName, workspaceId, fileStream, datasetName);

                    // Example of polling the import to check when the import has succeeded.
                    while (import.ImportState != "Succeeded" && import.ImportState != "Failed")
                    {
                        import = await client.Imports.GetImportByIdAsync(workspaceCollectionName, workspaceId, import.Id);
                        Console.ForegroundColor = ConsoleColor.Cyan;
                        Console.WriteLine("Checking import state... {0}", import.ImportState);
                        Thread.Sleep(1000);
                    }

                    return import;
                }
            }
        }

       

        /// <summary>
        /// Updates the Power BI dataset connection info for datasets with direct query connections
        /// </summary>
        /// <param name="workspaceCollectionName">The Power BI workspace collection name</param>
        /// <param name="workspaceId">The Power BI workspace id that contains the dataset</param>
        /// <returns></returns>
        static async Task UpdateConnection(string workspaceCollectionName, string workspaceId)
        {

            if (string.IsNullOrWhiteSpace(username))
            {
                Console.Write("SQL Azure Username: ");
                username = Console.ReadLine();
                
            }

            if (string.IsNullOrWhiteSpace(password))
            {
                Console.Write("SQL Azure Password: ");
                password = ConsoleHelper.ReadPassword();
                Console.WriteLine();
               
            }

            string connectionString = null;
            Console.Write("Connection String: ");
            connectionString = Console.ReadLine();
          

            using (var client = await CreateClient())
            {
                // Get the newly created dataset from the previous import process
                var datasets = await client.Datasets.GetDatasetsAsync(workspaceCollectionName, workspaceId);

                // Optionally udpate the connectionstring details if present
                if (!string.IsNullOrWhiteSpace(connectionString))
                {
                    var connectionParameters = new Dictionary<string, object>
                    {
                        { "connectionString", connectionString }
                    };
                    await client.Datasets.SetAllConnectionsAsync(workspaceCollectionName, workspaceId, datasets.Value[datasets.Value.Count - 1].Id, connectionParameters);
                }

                // Get the datasources from the dataset
                var datasources = await client.Datasets.GetGatewayDatasourcesAsync(workspaceCollectionName, workspaceId, datasets.Value[datasets.Value.Count - 1].Id);

                // Reset your connection credentials
                var delta = new GatewayDatasource
                {
                    CredentialType = "Basic",
                    BasicCredentials = new BasicCredentials
                    {
                        Username = username,
                        Password = password
                    }
                };

                // Update the datasource with the specified credentials
                await client.Gateways.PatchDatasourceAsync(workspaceCollectionName, workspaceId, datasources.Value[datasources.Value.Count - 1].GatewayId, datasources.Value[datasources.Value.Count - 1].Id, delta);
            }
        }

        /// <summary>
        /// Creates a new instance of the PowerBIClient with the specified token
        /// </summary>
        /// <returns></returns>
        static async Task<PowerBIClient> CreateClient()
        {
            if (accessKeys == null)
            {
                Console.Write("Access Key: ");
                accessKey = Console.ReadLine();
                Console.WriteLine();

                accessKeys = new WorkspaceCollectionKeys()
                {
                    Key1 = accessKey
                };
            }

            if (accessKeys == null)
            {
                accessKeys = await ListWorkspaceCollectionKeys(subscriptionId, resourceGroup, workspaceCollectionName);
            }

            // Create a token credentials with "AppKey" type
            var credentials = new TokenCredentials(accessKeys.Key1, "AppKey");

            // Instantiate your Power BI client passing in the required credentials
            var client = new PowerBIClient(credentials);

            // Override the api endpoint base URL.  Default value is https://api.powerbi.com
            client.BaseUri = new Uri(apiEndpointUri);

            return client;
        }

        static async Task<IEnumerable<string>> GetTenantIdsAsync(string commonToken)
        {
            using (var httpClient = new HttpClient())
            {
                httpClient.DefaultRequestHeaders.Add("Authorization", "Bearer " + commonToken);
                var response = await httpClient.GetStringAsync("https://management.azure.com/tenants?api-version=2016-01-29");
                var tenantsJson = JsonConvert.DeserializeObject<JObject>(response);
                var tenants = tenantsJson["value"] as JArray;

                return tenants.Select(t => t["tenantId"].Value<string>());
            }
        }

        /// <summary>
        /// Gets an Azure access token that can be used to call into the Azure ARM apis.
        /// </summary>
        /// <returns>A user token to access Azure ARM</returns>
        static async Task<string> GetAzureAccessTokenAsync()
        {
            if (!string.IsNullOrWhiteSpace(azureToken))
            {
                return azureToken;
            }

            var commonToken = GetCommonAzureAccessToken();
            var tenantId = (await GetTenantIdsAsync(commonToken.AccessToken)).FirstOrDefault();

            if (string.IsNullOrWhiteSpace(tenantId))
            {
                throw new InvalidOperationException("Unable to get tenant id for user accout");
            }

            var authority = string.Format("https://login.windows.net/{0}/oauth2/authorize", tenantId);
            var authContext = new AuthenticationContext(authority);
            var result = await authContext.AcquireTokenByRefreshTokenAsync(commonToken.RefreshToken, clientId, armResource);

            return (azureToken = result.AccessToken);

        }

        /// <summary>
        /// Gets a user common access token to access ARM apis
        /// </summary>
        /// <returns></returns>
        static AuthenticationResult GetCommonAzureAccessToken()
        {
            var authContext = new AuthenticationContext("https://login.windows.net/common/oauth2/authorize");
            var result = authContext.AcquireToken(
                resource: armResource,
                clientId: clientId,
                redirectUri: redirectUri,
                promptBehavior: PromptBehavior.Auto);

            if (result == null)
            {
                throw new InvalidOperationException("Failed to obtain the JWT token");
            }

            return result;
        }
    }
}
