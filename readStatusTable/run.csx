#r "Microsoft.WindowsAzure.Storage"
#r "Newtonsoft.Json"


using Microsoft.WindowsAzure.Storage.Table;
using System.Net;
using Newtonsoft.Json;
using System.Text.RegularExpressions;
using Newtonsoft.Json.Linq;


public static async Task<HttpResponseMessage> Run(HttpRequestMessage req, IQueryable<LogEntity> tableBinding, TraceWriter log)
{
    log.Info("HTTP status log trigger function received a request.");

    // Get request body
    dynamic data = await req.Content.ReadAsAsync<object>();

    var headers = req.Headers.ToString();
    log.Info(headers);
    // if(req.Headers["UserEmail"] != null)
    // {
    //     log.Info(req.Headers["UserEmail"]);
    // }

    var msgIds = data?.msgIds;

    foreach(string a in msgIds){
        log.Info($"ID: {a}");
    }
    
    List<LogReturn> logreturns = new List<LogReturn>();

    foreach(string msgId in msgIds){

        //Query Logs
        var logList = tableBinding.Where(r => r.RowKey == msgId).ToList();

        if (logList != null){

            foreach (LogEntity lg in logList)
            {
                logreturns.Add(new LogReturn{ msgId=lg.RowKey, success=lg.success, notes=lg.notes, timelastupdated=lg.Timestamp.ToString()});
                //log.Info(JsonConvert.SerializeObject(logreturns));
            }
        }
        else {
                logreturns.Add(new LogReturn{ msgId=msgId, notes="MessageID Not Found"});

        }

       
    }

    log.Info($"Returning {logreturns.Count.ToString()} status records");

    return logreturns.Count > 0
        ? req.CreateResponse(HttpStatusCode.OK, new { status = logreturns })
        :  req.CreateResponse(HttpStatusCode.BadRequest, "Error processing, or no valid message IDs found");
}


public class LogReturn 
{
    public string msgId { get; set; }
    public bool? success { get; set; }
    public string notes { get; set; }
    public string timelastupdated {get; set;}
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

//Sample Input Body
// {
// "msgIds":
//     [
//         "c0eccd3f-4a01-4dd9-a05b-9def0a004923",
//         "025b88a7-14f5-4aa9-93ce-e6d0a1d7fcaf"
//     ]
// }