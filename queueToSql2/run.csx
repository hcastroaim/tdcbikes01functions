#r "System.Configuration"
#r "System.Data"
#r "Newtonsoft.Json"
#r "Microsoft.WindowsAzure.Storage"
 
using System;
using System.Configuration;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Microsoft.WindowsAzure.Storage; 
using Microsoft.WindowsAzure.Storage.Table;

public static async Task Run(string myQueueItem, ICollector<LogInfo> outputTableRow, TraceWriter log)
{
    log.Info($"Queue trigger function initiated");
    

        Message oMsg = JsonConvert.DeserializeObject<Message>(myQueueItem);
        
        var connStr = ConfigurationManager.ConnectionStrings["SQLConnectionString"].ConnectionString;

        using (SqlConnection conn = new SqlConnection(connStr))
        {
            conn.Open();

            var text = $"INSERT INTO Events (bike_id, [time], [location], event_type, rider_id,rider_gender, rider_age, rider_resident, message_id) VALUES ('{oMsg.bike_id}', '{oMsg.time}', geography::STGeomFromText('POINT({oMsg.longitude} {oMsg.latitude})', 4326), '{oMsg.event_type}', '{oMsg.rider_info.riderId}', '{oMsg.rider_info.gender}', '{oMsg.rider_info.age}', '{oMsg.rider_info.resident}','{oMsg.msgId}')";

            using (SqlCommand cmd = new SqlCommand(text, conn))
            {
                var rows = await cmd.ExecuteNonQueryAsync();
            }
        }

        //log.Info(oMsg.ToString());
        var x = sendLog(oMsg.msgId, true, ""); 
        log.Info(x);

}

public static string sendLog (string msgId, bool success, string notes) {
    
    string returnmessage = "";
    //Initialize Connection
    CloudStorageAccount storageAccount = CloudStorageAccount.Parse(System.Configuration.ConfigurationManager.AppSettings["Queue-ConnectionString"]);
    CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
    CloudTable table = tableClient.GetTableReference("statuslogs");

    //Retrieve Current Log
    TableOperation retrieveOperation = TableOperation.Retrieve<LogEntity>("Events", msgId);
    TableResult retrievedResult = table.Execute(retrieveOperation);
    LogEntity updateEntity = (LogEntity)retrievedResult.Result;

    if (updateEntity != null)
    {
        // Changes.
        updateEntity.success = success;

        // Create the Replace TableOperation.
        TableOperation updateOperation = TableOperation.Replace(updateEntity);
        table.Execute(updateOperation);

    }
    else
    {
        //not Found
        returnmessage = "## Not Found";
    }

    return returnmessage;

}


public class Message
{
    public string msgId { get; set; }
    public string bike_id { get; set; }
    public DateTime time { get; set; }
    public double latitude { get; set; }
    public double longitude { get; set; }
    public string event_type { get; set; }
    public RiderInfo rider_info { get; set; }

}
public class RiderInfo
{
    public string riderId { get; set; }
    public string gender { get; set; }
    public int? age { get; set; }
    public int? resident { get; set; }
}

public class LogInfo
{
    public string PartitionKey { get; set; } = "Events";
    public string RowKey { get; set; }
    public bool? success { get; set; }
    public string notes { get; set; }
}


public class LogEntity : TableEntity {
    public LogEntity(string msgId)
    {
        this.PartitionKey = "Events";
        this.RowKey = msgId;
    }
    public LogEntity() {}
    public bool? success { get; set; }
    public string notes { get; set; } 
}