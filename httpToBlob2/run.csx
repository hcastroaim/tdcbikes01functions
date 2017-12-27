using System.Net;
using System.IO;
using System;
using CsvHelper;  
 
public static  HttpResponseMessage Run(HttpRequestMessage req, TraceWriter log, TextWriter outputBlob, IAsyncCollector<AuditLogMessage> outputQueueAuditLog, ICollector<LogInfo> outputTableRow)
{

    log.Info("HTTP blob trigger function received a request.");
    
    string userEmail;
    try {
        IEnumerable<string> headerValues = req.Headers.GetValues("UserEmail");
        userEmail = headerValues.FirstOrDefault();
    }
    catch{
        log.Info("No UserEmail Header");
        userEmail = "NA";
    }

    //Get HTTP Body
    var body = GetBody(req, log);
    var stream = body.Result;
    
    //Setup CSV File Reader
    TextReader textReader = new StreamReader(stream);  
    var csv = new CsvReader( textReader );
    
    //Initialize
    List<string> msgIdlist = new List<string>();
    List<Message> msglist = new List<Message>();
    
    int count1 = 0;
    int failurecount = 0;
    string msgGuid;
    int count = 0;
    var currentTime=DateTime.Now;

    //Create objects from list
    while( csv.Read() )
    {
        count1++;
        //Create and log message ID
        msgGuid = System.Guid.NewGuid().ToString();
        msgIdlist.Add(msgGuid);
        
        try
        {   
            var record = csv.GetRecord<MessagePartial>();

            //Add msgId
            Message m = MessageFromPartial(record,msgGuid);
            msglist.Add(m);
            //msgIdlist.Add(m.msgId);
            
            //Log
            outputTableRow.Add(
                new LogInfo {RowKey = m.msgId, success = null, notes="Adding to batch queue."}
            ); 
            
            
            outputQueueAuditLog.AddAsync(new AuditLogMessage{msgId=m.msgId, timeuploaded = currentTime, email = userEmail});
            
            count++;
        }
        catch (Exception ex)
        {
            //Will catch during csv data type mismatch or other errors
            log.Info($"Error on Record {count1}. Exception:");
            log.Info(ex.ToString());
            //Log failure to table 
            outputTableRow.Add(
                new LogInfo {RowKey = msgGuid, success = false, notes="Error when processing CSV. Check data type formats?"}
            );
            failurecount++;
        }   
    } //End of message loop

    //Write to blob
    var csvwrite = new CsvWriter (outputBlob);
    csvwrite.WriteRecords(msglist);

    //Finish
    log.Info($"{count} events written. {failurecount} errors. Done.");
    return req.CreateResponse(HttpStatusCode.OK, new ResponseMsgs { msgIds = msgIdlist });
}

public static async Task<Stream> GetBody(HttpRequestMessage req, TraceWriter log)
{
    Stream bodytext = await req.Content.ReadAsStreamAsync();
    return bodytext;
}

public static Message MessageFromPartial(MessagePartial m, string msgId)
{
    return new Message{
        msgId = msgId,
        bike_id = m.bike_id,
        time= m.time,
        latitude= m.latitude,
        longitude= m.longitude,
        event_type= m.event_type,
        rider_info= m.rider_info
    };
}

public class LogInfo
{
    public string PartitionKey { get; set; } = "Events";
    public string RowKey { get; set; }
    public bool? success { get; set; }
    public string notes { get; set; }
}

public class ResponseMsgs {
    public List<string> msgIds {get; set;}
}

public class Message
{
    public string msgId {get;set;}
    public string bike_id { get; set; }
    public DateTime time { get; set; }
    public float latitude { get; set; }
    public string longitude { get; set; }
    public string event_type { get; set; }
    public RiderInfo rider_info { get; set; }
}
public class MessagePartial
{

    public string bike_id { get; set; }
    public DateTime time { get; set; }
    public float latitude { get; set; }
    public string longitude { get; set; }
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

public class AuditLogMessage
{
    public string email { get; set; }
    public string api_called { get; set; } = "csv";
    public DateTime timeuploaded { get; set; }
    public string msgId { get; set; }
}