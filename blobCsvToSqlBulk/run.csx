#r "System.Configuration"
#r "System.Data"
#r "Newtonsoft.Json"
#r "Microsoft.WindowsAzure.Storage" 

 
using System.Net;
using System;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Microsoft.WindowsAzure.Storage; 
using Microsoft.WindowsAzure.Storage.Table;
using CsvHelper;

public static async Task Run(Stream myBlob, string name, TraceWriter log)
{
    string containerName = "csvpending";
    string filename = containerName  + "/" + name;
    
    log.Info($"C# Blob trigger function processing blob\n Name:{name} \n");
    
    var connStr = ConfigurationManager.ConnectionStrings["SQLAdminConnectionString"].ConnectionString;
    
    using (SqlConnection conn = new SqlConnection(connStr))
    {
        conn.Open();
        
        var text = "SP-BulkInsertFromBlob";
        var text2 = @"EXEC [dbo].[SP-MoveLatLongEvents]";

        using (SqlCommand cmd = new SqlCommand(text, conn))
        {
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Parameters.AddWithValue("@filename", filename);
            var stagetablecmd = await cmd.ExecuteNonQueryAsync();
                log.Info($"## Uploaded to Stage Table");
        }
        using (SqlCommand cmd2 = new SqlCommand(text2, conn))
        {
            var movecmd = await cmd2.ExecuteNonQueryAsync();
                log.Info($"## Stored Proc Run");
        }
    }

        //Logging loop
        //TODO: make more efficient by batching (slow-ish perf of ~40 records/second in current state)
        //List<string> msgIds = new List<string>();
        TextReader tr = new StreamReader(myBlob);
        var csv = new CsvReader( tr );
        var currentTime = DateTime.Now;
        while( csv.Read() )
        {
            var msgId = csv.GetField<string>( "msgId" );
            //msgIds.Add(msgId);
            sendLog(msgId, true, "");
        }
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
        updateEntity.notes = notes;

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

public class LogEntity : TableEntity
{
    public LogEntity(string pKey, string msgId)
    {
        this.PartitionKey = pKey;
        this.RowKey = msgId;
    }

    public LogEntity() { }

    public bool success { get; set; }
    public string notes { get; set; }
}

