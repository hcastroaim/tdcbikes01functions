#r "Newtonsoft.Json"
#r "Microsoft.WindowsAzure.Storage"
#r "System.Data"
#r "System.Configuration" 

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using System.Configuration;
using System.Data.SqlClient;
using Newtonsoft.Json;

public static void Run(TimerInfo myTimer, TraceWriter log)
{
    log.Info($"C# Timer trigger function executed at: {DateTime.Now}"); 

    // Retrieve queue storage reference from connection string.
    CloudStorageAccount storageAccount = CloudStorageAccount.Parse(System.Configuration.ConfigurationManager.AppSettings["Queue-ConnectionString"]);
    CloudQueueClient queueClient = storageAccount.CreateCloudQueueClient();
    CloudQueue queue = queueClient.GetQueueReference("auditlogqueue");

    int queueLength;
    queueLength = GetQueueLength(queue);

    while (queueLength > 0)
    {
        queueLength = GetQueueWriteSql(queue).GetAwaiter().GetResult();            
    }
}

public static async Task<int> GetQueueWriteSql(CloudQueue queue)
{
    var queueMessages = queue.GetMessages(30, TimeSpan.FromSeconds(30));

    List<AuditLogMessage> msgs = new List<AuditLogMessage>();
    
    //Get 25 messages at a time
    foreach (CloudQueueMessage message in queueMessages)
    {
        msgs.Add(JsonConvert.DeserializeObject<AuditLogMessage>(message.AsString));
    }

    int rows = await WriteSQL(msgs);

    if (rows > 0)
    {
        //Delete when done
        foreach (CloudQueueMessage msg in queueMessages)
        {
            queue.DeleteMessage(msg);
        }
        return GetQueueLength(queue);
    }
    else
    {
        return -1;
    }

}

public static int GetQueueLength(CloudQueue queue)
{
    // Fetch the queue attributes.
    queue.FetchAttributes();

    // Retrieve the cached approximate message count.
    int? cachedMessageCount = queue.ApproximateMessageCount;
    if (cachedMessageCount != null)
    {
        return (int)cachedMessageCount;
    }
    else
    {
        return 0;
    }
    
}

public static async Task<int> WriteSQL(List<AuditLogMessage> msgs)
{
    var json = JsonConvert.SerializeObject(msgs);
    var connStr = ConfigurationManager.ConnectionStrings["SQLConnectionString"].ConnectionString;
    int rows;

    using (SqlConnection conn = new SqlConnection(connStr))
    {
        conn.Open();

        var text = $"DECLARE @json nvarchar(max) = '{json}'; INSERT INTO AuditTable SELECT * FROM OPENJSON(@json) WITH(email nvarchar(100),api_called nvarchar(20),timeuploaded datetime2(7),msgId uniqueidentifier);";

        using (SqlCommand cmd = new SqlCommand(text, conn))
        {
            rows = await cmd.ExecuteNonQueryAsync();
        }
        
        conn.Close();
    }

    return rows;
}

public class AuditLogMessage
{
    public string email { get; set; }
    public string api_called { get; set; }
    public DateTime timeuploaded { get; set; }
    public string msgId { get; set; }
}