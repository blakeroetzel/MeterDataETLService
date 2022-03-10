using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data.Odbc;
using System.IO;
using System.Net;
using System.Timers;
using System.Net.Mail;
using System.Net.Sockets;
using Renci.SshNet;
using Renci.SshNet.Sftp;
using Renci.SshNet.Common;

namespace MeterDataETLService
{
    public partial class Service1 : ServiceBase
    {
        public Service1()
        {
            InitializeComponent();
        }
        Timer timer = new Timer();
        String startTime = " 12:00:00 AM";
        String versionNum = "1.0";
        String serviceName = "Meter Data ETL Service";
        Dictionary<string, string> Config = new Dictionary<string, string>();

        public void readConfig()
        {
            using (StreamReader sr = new StreamReader("C:/Dev/MeterDataETLService/config.conf"))
            {
                while (sr.Peek() >= 0)
                {
                    string line = sr.ReadLine();
                    var linesplit = line.Split('=');
                    Config[linesplit[0].Trim()] = linesplit[1].Trim();
                }
            }
        }

        private long fileSize { get; set; }

        private void HandleKeyEvent(object sender, AuthenticationPromptEventArgs e)
        {
            foreach (AuthenticationPrompt prompt in e.Prompts)
            {
                if (prompt.Request.IndexOf("Password:", StringComparison.InvariantCultureIgnoreCase) != -1)
                {
                    prompt.Response = Config["APIPassword"];
                }
            }
        }

        public void update()
        {

            KeyboardInteractiveAuthenticationMethod keybAuth = new KeyboardInteractiveAuthenticationMethod(Config["APIUser"]);
            keybAuth.AuthenticationPrompt += new EventHandler<AuthenticationPromptEventArgs>(HandleKeyEvent);

            ConnectionInfo conInfo = new ConnectionInfo(Config["APIServer"], Config["APIUser"], keybAuth);

            using (SftpClient sftp = new SftpClient(conInfo))
            {
                try
                {
                    WriteToFile("Connecting to sftp server...");
                    sftp.Connect();
                    WriteToFile("Connected");

                    var filename = sftp.ListDirectory(@"/outgoing/");
                    var names = from fn in filename where fn.Name.Contains("intervals") select (fn.Name, fn.LastWriteTime);
                    var lastDate = names.Max(x => x.LastWriteTime);
                    var latestFile = names.FirstOrDefault(i => i.LastWriteTime == lastDate).Name;
                    Console.WriteLine(latestFile);

                    WriteToFile("Downloading file...");

                    using (var Stream = new FileStream(@"C:/Dev/intervaldata.csv", FileMode.Create))
                    {
                        fileSize = sftp.GetAttributes($@"/outgoing/{latestFile}").Size;
                        sftp.DownloadFile($@"/outgoing/{latestFile}", Stream);
                    }

                    WriteToFile("File Downloaded");

                    sftp.Disconnect();

                    DataTable dt = new DataTable();
                    dt.Columns.Add("MeterID", System.Type.GetType("System.Int32"));
                    dt.Columns.Add("Interval_Start", System.Type.GetType("System.DateTime"));
                    dt.Columns.Add("Interval_End", System.Type.GetType("System.DateTime"));
                    dt.Columns.Add("Delivered", System.Type.GetType("System.Double"));
                    dt.Columns.Add("Received", System.Type.GetType("System.Double"));
                    GC.Collect();
                    WriteToFile("Pivoting Data...");
                    using (var Stream = new FileStream(@"C:/Dev/intervaldata.csv", FileMode.Open))
                    using (var reader = new StreamReader(Stream))
                    {
                        int row = 0;
                        string line;
                        try
                        {
                            long meterID = 0;
                            Pivoter pivoter = new Pivoter();
                            while ((line = reader.ReadLine()) != null)
                            {
                                if (row == 0)
                                {
                                    row++;
                                    continue;
                                }

                                var columns = line.Split('~');

                                long newMeterID = long.Parse(columns[6]);

                                if (meterID != newMeterID)
                                {
                                    if (row != 1)
                                    {
                                        foreach (DataRow r in pivoter.dataRows)
                                        {
                                            dt.Rows.Add(r);
                                        }
                                    }
                                    meterID = newMeterID;
                                    pivoter = new Pivoter(meterID, columns[13], dt);
                                }

                                string unit = columns[9];

                                switch (unit)
                                {
                                    case "KWH":
                                        for (int i = 0; i < 96; i++)
                                        {
                                            int index = (i * 2) + 15;
                                            pivoter.addData(columns[index], index, "Delivered");
                                        }
                                        break;
                                    case "-KWH":
                                        for (int i = 0; i < 96; i++)
                                        {
                                            int index = (i * 2) + 15;
                                            pivoter.addData(columns[index], index, "Received");
                                        }
                                        break;
                                    default:
                                        break;
                                }

                                row++;
                            }

                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                            return;
                        }
                    }

                    WriteToFile("Data Pivoted.");
                    WriteToFile("Uploading to SQL Server...");

                    using (var bulkCopy = new SqlBulkCopy(Config["ODBCString"]))
                    {
                        foreach (DataColumn col in dt.Columns)
                        {
                            bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                        }
                        bulkCopy.BulkCopyTimeout = 600;
                        bulkCopy.DestinationTableName = Config["TableName"];
                        try
                        {
                            bulkCopy.WriteToServer(dt);
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                            return;
                        }
                    }

                    WriteToFile("Upload Complete.");
                    WriteToFile("Deleting Local File...");

                    // Delete Local file
                    File.Delete(@"C:/Dev/intervaldata.csv");

                    WriteToFile("File Deleted.");

                    return;
                }
                catch (Exception e)
                {
                    WriteToFile($"Exception occoured: {e}");
                    return;
                }
            }
        }

        private void onElapsedTime(object source, ElapsedEventArgs e)
        {
            WriteToFile(serviceName + " running");
            update();
            GC.Collect();
            int milli = getNextRun();
            timer.Enabled = false;
            timer.Interval = milli;
            WriteToFile("Next updating at " + DateTime.Now.AddMilliseconds(milli));
            timer.Enabled = true;
        }

        private int getNextRun()
        {
            DateTime dt1 = DateTime.Now;
            DateTime dt2 = DateTime.Parse(DateTime.Now.AddDays(1).ToString("MM/dd/yyyy") + startTime);
            TimeSpan span = dt2 - dt1;
            int milli = (int)span.TotalMilliseconds;
            return milli;
        }

        private int getToday()
        {
            DateTime dt1 = DateTime.Now;
            DateTime dt2 = DateTime.Parse(DateTime.Now.ToString("MM/dd/yyyy") + startTime);
            TimeSpan span = dt2 - dt1;
            int milli = (int)span.TotalMilliseconds;
            return milli;
        }

        public void OnDebug()
        {
            update();
            OnStart(null);
        }
        protected override void OnStart(string[] args)
        {
            WriteToFile($"{serviceName} v{versionNum} starting");
            timer.Elapsed += new ElapsedEventHandler(onElapsedTime);
            int milli = new int();
            if (DateTime.Now < DateTime.Parse(DateTime.Now.ToString("MM/dd/yyyy") + startTime))
            {
                milli = getToday();
            }
            else
            {
                milli = getNextRun();
            }
            timer.Interval = milli;
            WriteToFile("Next updating at " + DateTime.Now.AddMilliseconds(milli));
            timer.Enabled = true;
        }

        protected override void OnStop()
        {
            WriteToFile(serviceName + " stopping");
        }
        public void WriteToFile(string Message)
        {
            string path = "C:\\Dev\\";
            if (!Directory.Exists(path))
            {
                Directory.CreateDirectory(path);
            }

            // Update This Name!
            string filepath = "C:\\Dev\\MeterDataETLServiceLog.txt";
            string datetimenow = DateTime.Now.ToString() + ":\t";
            if (!File.Exists(filepath))
            {
                using (StreamWriter sw = File.CreateText(filepath))
                {
                    sw.WriteLine(datetimenow + Message);
                }
            }
            else
            {
                using (StreamWriter sw = File.AppendText(filepath))
                {
                    sw.WriteLine(datetimenow + Message);
                }
            }
        }
    }
}
