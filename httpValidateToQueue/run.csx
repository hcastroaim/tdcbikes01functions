#r "Newtonsoft.Json"
#r "Microsoft.WindowsAzure.Storage"

using System.Net;
using System;
using Newtonsoft.Json;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;


public static HttpResponseMessage Run(HttpRequestMessage req, TraceWriter log, out string outputQueueItem, out string outputQueueAuditLog, ICollector<LogInfo> outputTableRow)
{
    string messageId = Guid.NewGuid().ToString();
    string userEmail;
    try {
        IEnumerable<string> headerValues = req.Headers.GetValues("UserEmail");
        userEmail = headerValues.FirstOrDefault();
    }
    catch{
        log.Info("No UserEmail Header");
        userEmail = "NA";
    }

    try
    {
        log.Info("HTTP trigger function received a request.");
        
        var body = GetBody(req, log);
        var bodystring = body.Result;

        Message oMsg = JsonConvert.DeserializeObject<Message>(bodystring);
        oMsg.msgId = messageId;

        log.Info($"New Message Added to Queue, ID: {oMsg.msgId}");

        outputQueueItem = JsonConvert.SerializeObject(oMsg);
        outputQueueAuditLog = JsonConvert.SerializeObject(new AuditLogMessage {email = userEmail, msgId=oMsg.msgId, timeuploaded = DateTime.Now});
        
        outputTableRow.Add(
            new LogInfo {RowKey = oMsg.msgId, success = null, notes=""}
        );


        return req.CreateResponse(HttpStatusCode.OK, new ResponseMsg {msgId = oMsg.msgId});
    }
    
    catch(Exception ex) 
    {
        outputQueueItem = null;
        log.Info(ex.ToString());
        outputTableRow.Add(
            new LogInfo {RowKey = messageId, success = false, notes = ex.Message}
        );
        outputQueueAuditLog = null;        
        return req.CreateResponse(HttpStatusCode.BadRequest, $"Error. Message ID {messageId}");
        
    }
}

public static async Task<string> GetBody(HttpRequestMessage req, TraceWriter log)
{
    dynamic data = await req.Content.ReadAsAsync<object>();
    return data.ToString();
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

public class ResponseMsg {
    public string msgId {get; set;}
}

public class AuditLogMessage
{
    public string email { get; set; }
    public string api_called { get; set; } = "single";
    public DateTime timeuploaded { get; set; }
    public string msgId { get; set; }
}
