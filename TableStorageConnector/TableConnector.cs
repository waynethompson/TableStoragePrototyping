﻿using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;

namespace TableStorageConnector
{
    public class TableConnector<T> where T : ITableEntity, new()
    {
        public TableConnector(AzureTableSettings settings)
        {
            this.settings = settings;
        }

        public async Task<List<T>> GetFullList(string tableName = "")
        {          
            //Table
            CloudTable table = await GetTableAsync(tableName);

            //Query
            TableQuery<T> query = new TableQuery<T>();

            List<T> results = new List<T>();
            TableContinuationToken continuationToken = null;
            do
            {
                TableQuerySegment<T> queryResults =
                    await table.ExecuteQuerySegmentedAsync(query, continuationToken);

                continuationToken = queryResults.ContinuationToken;
                results.AddRange(queryResults.Results);

            } while (continuationToken != null);

            return results;
        }

        public async Task<List<T>> GetList(string partitionKey, string tableName = "")
        {
            //Table
            CloudTable table = await GetTableAsync(tableName);

            //Query
            TableQuery<T> query = new TableQuery<T>()
                                        .Where(TableQuery.GenerateFilterCondition("PartitionKey",
                                                QueryComparisons.Equal, partitionKey));

            List<T> results = new List<T>();
            TableContinuationToken token = null;
            do
            {
                TableQuerySegment<T> queryResults = await table.ExecuteQuerySegmentedAsync(query, token);

                token = queryResults.ContinuationToken;

                results.AddRange(queryResults.Results);

            } while (token != null);

            return results;
        }

        public async Task<T> GetItem(string partitionKey, string rowKey, string tableName = "")
        {
            //Table
            CloudTable table = await GetTableAsync(tableName);

            //Operation
            TableOperation operation = TableOperation.Retrieve<T>(partitionKey, rowKey);

            //Execute
            TableResult result = await table.ExecuteAsync(operation);

            return (T)(dynamic)result.Result;
        }

        public async Task Insert(T item, string tableName = "")
        {
            //Table
            CloudTable table = await GetTableAsync(tableName);

            //Operation
            TableOperation operation = TableOperation.Insert(item);

            //Execute
            await table.ExecuteAsync(operation);
        }

        public async Task Insert(IList<T> items, string tableName = "")
        {
            //Table
            CloudTable table = await GetTableAsync(tableName);

            // Create the batch operation.
            TableBatchOperation batchOperation = new TableBatchOperation();

            foreach (var item in items)
            {
                batchOperation.Insert(item);
            }

            await table.ExecuteBatchAsync(batchOperation);
        }

        public async Task Update(T item, string tableName = "")
        {
            //Table
            CloudTable table = await GetTableAsync(tableName);

            //Operation
            TableOperation operation = TableOperation.InsertOrReplace(item);

            //Execute
            await table.ExecuteAsync(operation);
        }

        public async Task Delete(string partitionKey, string rowKey, string tableName = "")
        {
            //Item
            T item = await GetItem(partitionKey, rowKey);

            //Table
            CloudTable table = await GetTableAsync(tableName);

            //Operation
            TableOperation operation = TableOperation.Delete(item);

            //Execute
            await table.ExecuteAsync(operation);
        }

        #region "Private"

        public readonly AzureTableSettings settings;

        private async Task<CloudTable> GetTableAsync(string tableName = "")
        {
            if(string.IsNullOrWhiteSpace(tableName)){
                tableName = this.settings.TableName;
            }
            //Account
            CloudStorageAccount storageAccount = new CloudStorageAccount(
                new StorageCredentials(this.settings.StorageAccount, this.settings.StorageKey), false);

            //Client
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            //Table
            CloudTable table = tableClient.GetTableReference(tableName);
            await table.CreateIfNotExistsAsync();

            return table;
        }
        #endregion
    }
}