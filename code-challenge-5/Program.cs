using System;
using Azure;
using System.Threading.Tasks;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.Azure.Storage;

namespace storage_lab
{
    class Program
    {
        private static string containerName="cars-and-engines";
        private static string blobName="car_engine_1.jpg";
        private static string connectionString="[PUT_THE_STORAGE_CONNECTION_STRING_HERE]";

        static void Main(string[] args)
        {
            BlobMenu();
        }
        
        private static async void BlobMenu()
        {
            ConsoleKeyInfo key; 

            do{
                Console.WriteLine("");
                Console.WriteLine("Enter 'q' to exist.");
                Console.WriteLine("Enter '1' to list all blobs.");
                Console.WriteLine("Enter '2' to list properties of 'car_engine_1.jpg' blob.");
                Console.WriteLine("Enter '3' to list metadata of 'car_engine_1.jpg' blob.");
                Console.WriteLine("Enter '4' to add a new metadata to 'car_engine_1.jpg' blob.");
                Console.WriteLine("Enter '5' to delete an existing metadata from 'car_engine_1.jpg' blob.");

                key = Console.ReadKey();

                switch(key.KeyChar)
                {
                    case '1':
                    Console.WriteLine("");
                        ListBlobsAsync();
                        break;
                    case '2':
                        Console.WriteLine("");
                        ListBlobProperties();
                        break;
                    case '3':
                        Console.WriteLine("");
                        ListBlobMetadata();
                        break;
                    case '4':
                        Console.WriteLine("");
                        Console.WriteLine("Enter metadata key (lowercase, no special chars):");
                        string keyToAdd = Console.ReadLine();
                        Console.WriteLine("Enter metadata value (lowercase, no special chars):");
                        string valueToAdd = Console.ReadLine();
                        SetBlobMetadata(keyToAdd,valueToAdd);
                        break;
                    case '5':
                        Console.WriteLine("");
                        Console.WriteLine("Enter existing metadata key to delete (lowercase, no special chars):");
                        string ketToDelete = Console.ReadLine();
                        DeleteBlobMetadata(ketToDelete);
                        break;                                                                               
                    default:
                    break;
                }
            }while(key.KeyChar !='q');
        }

        private static void ListBlobProperties()
        {
            try
            {
                BlobContainerClient containerClient = new BlobContainerClient(connectionString, containerName);

                BlobClient blobClient =  containerClient.GetBlobClient(blobName);

                // Get the blob properties
                BlobProperties properties = blobClient.GetProperties();

                // Display some of the blob's property values
                Console.WriteLine("-----------------------------------");
                Console.WriteLine($" Blob name: {blobClient.Name} - (System Properties)");
                Console.WriteLine($" BlobType: {properties.BlobType}");
                Console.WriteLine($" ContentType: {properties.ContentType}");
                Console.WriteLine($" CreatedOn: {properties.CreatedOn}");
                Console.WriteLine($" LastModified: {properties.LastModified}");
                Console.WriteLine($" IsLatestVersion: {properties.IsLatestVersion}");
                Console.WriteLine("-----------------------------------");                
            }
            catch (RequestFailedException e)
            {
                Console.WriteLine($"HTTP error code {e.Status}: {e.ErrorCode}");
                Console.WriteLine(e.Message);
                Console.ReadLine();
            }
        }


        private static void ListBlobMetadata()
        {
            try
            {
                BlobContainerClient containerClient = new BlobContainerClient(connectionString, containerName);

                BlobClient blobClient =  containerClient.GetBlobClient(blobName);

                // Get the blob properties
                BlobProperties properties = blobClient.GetProperties();

                // Display  blob's metadata values
                Console.WriteLine("-----------------------------------");
                Console.WriteLine($" Blob name: {blobClient.Name} - (Metadata)");
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

        private static bool SetBlobMetadata(string key, string value)
        {
            try
            {
                BlobContainerClient containerClient = new BlobContainerClient(connectionString, containerName);

                BlobClient blobClient =  containerClient.GetBlobClient(blobName);

                // Get the blob properties
                BlobProperties properties = blobClient.GetProperties();

                // Add  blob's metadata
                properties.Metadata.Add (key,value);

                blobClient.SetMetadata(properties.Metadata);

                Console.WriteLine("-----------------------------------");
                Console.WriteLine($" Metadata pair {{ {key} - {value} }} added to blob");
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

        private static bool DeleteBlobMetadata(string key)
        {
            try
            {
                BlobContainerClient containerClient = new BlobContainerClient(connectionString, containerName);

                BlobClient blobClient =  containerClient.GetBlobClient(blobName);

                // Get the blob properties
                BlobProperties properties = blobClient.GetProperties();

                // Add  blob's metadata
                if(!properties.Metadata.ContainsKey (key)) {
                    System.Console.WriteLine($"Metadata with key {{{key}}} doesn't exist in the collection.");
                    return false;
                }

                properties.Metadata.Remove(key);
                
                blobClient.SetMetadata(properties.Metadata);

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

        private static void ListBlobsAsync()
        {
            string continuationToken = null;
            try
            {
                BlobContainerClient containerClient = new BlobContainerClient(connectionString, containerName);

                // Call the listing operation and enumerate the result segment.
                // When the continuation token is empty, the last segment has been returned
                // and execution can exit the loop.
                do
                {
                    var resultSegment =containerClient.GetBlobs().AsPages(continuationToken,1);

                    foreach (Azure.Page<BlobItem> blobPage in resultSegment)
                    {
                        foreach (BlobItem blobItem in blobPage.Values)
                        {
                            Console.WriteLine($"Blob name: {blobItem.Name}, Modified: {blobItem.Properties.LastModified.Value}" +
                                $" AccessTier: {blobItem.Properties.AccessTier.Value}, LeaseState: {blobItem.Properties.LeaseState.Value}");
                        }

                        // Get the continuation token and loop until it is empty.
                        continuationToken = blobPage.ContinuationToken;

                        Console.WriteLine();
                    }

                } while (continuationToken != "");

            }
            catch (RequestFailedException e)
            {
                Console.WriteLine($"HTTP error code {e.Status}: {e.ErrorCode}");
                Console.WriteLine(e.Message);
                Console.ReadLine();
            }
        }
    }
}
