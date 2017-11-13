using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.IdentityModel.Protocols;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using TableStorageConnector;

namespace TableStoragePrototyping.Controllers
{
    [Route("api/[controller]")]
    public class TableController : Controller
    {
        private TableConnector<DynamicTableEntity> table;

        public TableController(TableConnector<DynamicTableEntity> _table)
        {
            table = _table;
        }

        // GET api/test
        [HttpGet("/{tableName}")]
        public async Task<ContentResult> Get(string tableName)
        {
            var results = await this.table.GetFullList(tableName);
            return Content(JsonConvert.SerializeObject(results), "application/json");
            //return await this.table.GetFullList(tableName);
        }

        // GET api/values/5
        [HttpGet("{tableName}/{partitionKey}/{rowKey}")]
        public async Task<DynamicTableEntity> Get(string tableName, string partitionKey, string rowKey)
        {
            return await this.table.GetItem(partitionKey, rowKey, tableName);
        }

        // POST api/values
        [HttpPost("{tableName}")]
        public async Task Post([FromBody]dynamic value, string tableName)
        {
            var entity = new DynamicTableEntity();

            //var d = JsonConvert.DeserializeObject<Dictionary<string, dynamic>>(value);

            //foreach (KeyValuePair<string, object> prop in value)
            //{
            //    if (prop.Value!=null)
            //    {
            //        entity.Properties.Add(prop.Key, (EntityProperty)prop.Value);
            //    }
            //}

            foreach (JProperty key in value)
            {
                if (!string.IsNullOrEmpty(key.Value.ToString()))
                {
                    entity.Properties.Add(key.Name, new EntityProperty(key.Value.ToString()));
                }
            }

            await this.table.Insert(entity, tableName);
        }

        // PUT api/values/5
        [HttpPut("{id}")]
        public async Task Put(int id, [FromBody]DynamicTableEntity value, string tableName)
        {
            await this.table.Update(value, tableName);
        }

        // DELETE api/table/test/test/
        [HttpDelete("{tableName}/{partitionKey}/{rowKey}")]
        public async Task Delete(string tableName, string partitionKey, string rowKey)
        {
            await this.table.Delete(partitionKey, rowKey, tableName);
        }
    }
}
