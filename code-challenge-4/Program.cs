using System;
using System.Threading.Tasks;
using System.Collections.Generic;

using Azure;
using Azure.Storage.Blobs;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace storage_lab
{
    class Program
    {
        private static string containerName="cars-and-engines";        
        private static string connectionString="DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=psblobstorageaccount;AccountKey=KFzJrWFZtE7mX6chov8sBCICHKS9x6RzcPaVtW5UlWJC7P3SZ61Ssyq5QAr7sRD7uMgmCWJpEG86MzlLXd/dqA==";

        static void Main(string[] args)
        {
            ContainerMenu();
        }

        private static async void ContainerMenu()
        {
            ConsoleKeyInfo key; 

            do{
                Console.WriteLine("");
                Console.WriteLine("Enter 'q' to exist.");
                Console.WriteLine("Enter '1' to list all containers.");
                Console.WriteLine("Enter '2' to list properties of 'cars_and_engines' container.");
                Console.WriteLine("Enter '3' to list metadata of 'cars_and_engines' container.");
                Console.WriteLine("Enter '4' to add a new metadata to 'cars_and_engines' container.");
                Console.WriteLine("Enter '5' to delete an existing metadata from 'cars_and_engines' container.");

                key = Console.ReadKey();

                switch(key.KeyChar)
                {
                    case '1':
                        Console.WriteLine("");
                        ListContainersAsync().Wait();
                        break;
                    case '2':
                        Console.WriteLine("");
                        ListContainerProperties();
                        break;
                    case '3':
                        Console.WriteLine("");
                        ListContainerMetadata();
                        break;
                    case '4':
                        Console.WriteLine("");
                        Console.WriteLine("Enter metadata key (lowercase, no special chars, no spaces):");
                        string keyToAdd = Console.ReadLine();
                        Console.WriteLine("Enter metadata value (lowercase, no special chars, no spaces):");
                        string valueToAdd = Console.ReadLine();
                        SetContainerMetadata(keyToAdd,valueToAdd);
                        break;
                    case '5':
                        Console.WriteLine("");
                        Console.WriteLine("Enter existing metadata key to delete (lowercase, no special chars):");
                        string ketToDelete = Console.ReadLine();
                        DeleteContainerMetadata(ketToDelete);
                        break;                                                                               
                    default:
                    break;
                }
            }while(key.KeyChar !='q');
        }

        private static void ListContainerProperties()
        {
            try
            {
                BlobContainerClient containerClient = new BlobContainerClient(connectionString, containerName);

                // Get the container properties
                Azure.Storage.Blobs.Models.BlobContainerProperties properties = containerClient.GetProperties();

                // Display some of the container's property values
                Console.WriteLine("-----------------------------------");
                Console.WriteLine($" Container name: {containerClient.Name} - (System Properties)");
                Console.WriteLine($" LastModified: {properties.LastModified}");
                Console.WriteLine($" LeaseState: {properties.LeaseState}");
                Console.WriteLine($" PublicAccess: {properties.PublicAccess}");
                Console.WriteLine("-----------------------------------");                
            }
            catch (RequestFailedException e)
            {
                Console.WriteLine($"HTTP error code {e.Status}: {e.ErrorCode}");
                Console.WriteLine(e.Message);
                Console.ReadLine();
            }
        }


        private static void ListContainerMetadata()
        {
            try
            {
                BlobContainerClient containerClient = new BlobContainerClient(connectionString, containerName);

                // Get the container properties
                Azure.Storage.Blobs.Models.BlobContainerProperties properties = containerClient.GetProperties();

                // Display container's metadata values
                Console.WriteLine("-----------------------------------");
                Console.WriteLine($" Container name: {containerClient.Name} - (Metadata)");
                foreach(var entry in properties.Metadata){
                    Console.WriteLine($" Metadata: {{key: {entry.Key} - Value: {entry.Value}}}");    
                }
                Console.WriteLine("-----------------------------------");
            }
            catch (RequestFailedException e)
            {
                Console.WriteLine($"HTTP error code {e.Status}: {e.ErrorCode}");
                Console.WriteLine(e.Message);
                Console.ReadLine();
            }
        }

        private static bool SetContainerMetadata(string key, string value)
        {
            try
            {
                BlobContainerClient containerClient = new BlobContainerClient(connectionString, containerName);

                // Get the container properties
                Azure.Storage.Blobs.Models.BlobContainerProperties properties = containerClient.GetProperties();

                // Add container's metadata
                properties.Metadata.Add (key,value);

                containerClient.SetMetadata(properties.Metadata);

                Console.WriteLine("-----------------------------------");
                Console.WriteLine($" Metadata pair {{ {key} - {value} }} added to container.");
                Console.WriteLine("-----------------------------------");      

                return true;
            }
            catch (RequestFailedException e)
            {
                Console.WriteLine($"HTTP error code {e.Status}: {e.ErrorCode}");
                Console.WriteLine(e.Message);
                Console.ReadLine();
            }

            return false;
        }

        private static bool DeleteContainerMetadata(string key)
        {
            try
            {
                BlobContainerClient containerClient = new BlobContainerClient(connectionString, containerName);

                // Get the container properties
                Azure.Storage.Blobs.Models.BlobContainerProperties properties = containerClient.GetProperties();

                // Check if a metadata with specified key exists.
                if(!properties.Metadata.ContainsKey (key)) {
                    System.Console.WriteLine($"Metadata with key {{{key}}} doesn't exist in the collection.");
                    return false;
                }

                properties.Metadata.Remove(key);
                
                containerClient.SetMetadata(properties.Metadata);

                Console.WriteLine("-----------------------------------");
                Console.WriteLine($" Metadata pair with key {{{key}}} has been deleted.");
                Console.WriteLine("-----------------------------------");      

                return true;  
            }
            catch (RequestFailedException e)
            {
                Console.WriteLine($"HTTP error code {e.Status}: {e.ErrorCode}");
                Console.WriteLine(e.Message);
                Console.ReadLine();
            }

            return false;
        }

        private static async Task<bool> ListContainersAsync()
        {
            try
            {
                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);

                storageAccount.CreateCloudBlobClient();
                var blobClient = storageAccount.CreateCloudBlobClient();
                var blobContainers = new List<CloudBlobContainer>();
                BlobContinuationToken blobContinuationToken = null;
                do
                {
                    var containerSegment = await blobClient.ListContainersSegmentedAsync(blobContinuationToken);
                    blobContainers.AddRange(containerSegment.Results);
                    blobContinuationToken = containerSegment.ContinuationToken;
                } while (blobContinuationToken != null);

                Console.WriteLine("-----------------------------------");    

                foreach(var blobContainer in blobContainers)
                {    
                    Console.WriteLine($"Container name: {blobContainer.Name}, PublicAccess: {blobContainer.Properties.PublicAccess} " 
                    + $",LastModified {blobContainer.Properties.LastModified}");
                }

                Console.WriteLine("-----------------------------------");

                return await Task.FromResult(true);
            }
            catch (RequestFailedException e)
            {
                Console.WriteLine($"HTTP error code {e.Status}: {e.ErrorCode}");
                Console.WriteLine(e.Message);
                Console.ReadLine();
            }

           return await Task.FromResult(false);
        }
    }
}
