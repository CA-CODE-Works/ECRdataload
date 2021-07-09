using Microsoft.Extensions.Configuration;
using PgpCore;
using Polly;
using Renci.SshNet;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Net.Mail;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;

namespace ECRdataload
{
    class Program
    {
        static SqlConnection con;
        static string errorFileName = "ECR_Upload_Errors" + DateTime.Now.ToString("yyyMMdd_HH") + ".csv";
        static string errorTestFileName = "PreProd_ECR_Upload_Errors" + DateTime.Now.ToString("yyyMMdd_HH") + ".csv";
        static private void connection(string dbcon)
        {
            con = new SqlConnection(dbcon);
        }
        static void Main(string[] args)
        {
            bool isFullFile = false;
            // if passing Full argument, perform full data load
            if (args.Length > 0 && args[0].Equals("Full"))
                isFullFile = true;

            bool isDevelopment = false;
            var devEnvironmentVariable = Environment.GetEnvironmentVariable("NETCORE_ENVIRONMENT");

            if (!string.IsNullOrEmpty(devEnvironmentVariable) && devEnvironmentVariable.ToLower() == "development")
                isDevelopment = true;
            //Determines the working environment as IHostingEnvironment is unavailable in a console app

            var builder = new ConfigurationBuilder();
            // tell the builder to look for the appsettings.json file
            builder
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);

            //only add secrets in development
            if (isDevelopment)
            {
                builder.AddUserSecrets<Program>();
            }


            // get config values from appsettings.json
            var config = builder.Build();
            string ConnectionString = config["ConnectionString"];
            int Retry = Int32.Parse(config["FtpSite:Retry"]);
            int WaitSec = Int32.Parse(config["FtpSite:WaitSec"]);
            string mailHost = config["Smtp:Server"];
            string toCDTAddress = config["Smtp:ToCDTAddress"];
            string toAddress = config["Smtp:ToAddress"];
            string databaseEnv = config["Smtp:DatabaseEnv"];
            int mailPort = Int32.Parse(config["Smtp:Port"]);
            string ftpFileName = databaseEnv.Length > 0 ? errorTestFileName : errorFileName;
            MailMessage message = new MailMessage(config["Smtp:FromAddress"], toCDTAddress);

            // get DB connection
            connection(ConnectionString);

            Stream employeedataStream = new MemoryStream();
            Stream decryptFileStream = new MemoryStream();

            DateTime lastmodified = new DateTime();
            try
            {
                // get CalHR transaction file and get the file modified date
                Policy
                    .Handle<Exception>()
                    .WaitAndRetry(Retry, retryAttempt => TimeSpan.FromSeconds(WaitSec))
                    .Execute(() => lastmodified = downloadFtpFile(employeedataStream, config, isFullFile));
                DateTime lastLoadedDt = getLoadDate();

                // Do not process if no new transaction file found, unless it is a full data load
                if (lastmodified > lastLoadedDt || isFullFile)
                {
                    // Decrypt trsansaction data 
                    using (PGP pgp = new PGP())
                    {
                        String pgpPassword = config["pgpPassword"];
                        employeedataStream.Seek(0, SeekOrigin.Begin);

                        using (Stream privateKeyStream = new FileStream(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location) + @"\public.asc", FileMode.Open))
                            pgp.DecryptStream(employeedataStream, decryptFileStream, privateKeyStream, pgpPassword);
                    }

                    // load data into EmployeeCoreRecord database. Send e-mail if error found.
                    int errorCount = updateDatabase(decryptFileStream, ConnectionString, config, isFullFile, ftpFileName);
                    if (errorCount > 0)
                    {
                        message.Subject = "ECR Input Transaction Errors" + databaseEnv;
                        message.IsBodyHtml = true;
                        message.To.Add(new MailAddress(toAddress));
                        message.Body = "This e-mail has been sent to you because there were " + errorCount + " error(s) with the latest import of the " +
                            "Employee Core Record input file. To view the errors, please visit the CDT Inbound account folder at the CalHR FTP site and open: "
                            + ftpFileName;
                        sendMail(mailHost, mailPort, message);
                    }
                }
                else
                {
                    message.Subject = "ECR New Transaction File Missing" + databaseEnv;
                    message.IsBodyHtml = true;
                    message.To.Add(new MailAddress(toAddress));
                    message.Body = "No new " + config["FtpSite:DownloadFileName"] + " file found.";
                    sendMail(mailHost, mailPort, message);
                }

            }
            catch (Exception e)
            {
                message.Subject = "ECR Data Load Exception" + databaseEnv;
                message.IsBodyHtml = true;
                message.Body = e.Message + "<br>" + e.StackTrace;
                sendMail(mailHost, mailPort, message);
            }
        }

        // Download transaction file from CalHR
        private static DateTime downloadFtpFile(Stream datastream, IConfigurationRoot ftpconfig, bool isFullFile)
        {
            using (var sftp = new SftpClient(ftpconfig["FtpSite:Host"], Int32.Parse(ftpconfig["FtpSite:Port"]), ftpconfig["FtpSite:Username"], ftpconfig["FtpSite:Password"]))
            {
                sftp.Connect();
                DateTime modft = new DateTime();
                string downloadfile = ftpconfig["FtpSite:DownloadDirectory"] + "/" + ftpconfig["FtpSite:DownloadFileName"];
                if (sftp.Exists(downloadfile))
                {
                    sftp.ChangeDirectory(ftpconfig["FtpSite:DownloadDirectory"]);
                    string downloadtype = ftpconfig["FtpSite:DownloadType"];
                    if (isFullFile)
                        sftp.DownloadFile(ftpconfig["FtpSite:DownloadFullFileName"], datastream);
                    else
                        sftp.DownloadFile(ftpconfig["FtpSite:DownloadFileName"], datastream);
                    modft = sftp.GetLastWriteTime(ftpconfig["FtpSite:DownloadFileName"]);
                }
                sftp.Disconnect();
                return modft;
            }
        }

        // Upload error file back to CalHR
        private static void uploadFtpFile(Stream datastream, IConfigurationRoot ftpconfig, string fileName)
        {
            using (var sftp = new SftpClient(ftpconfig["FtpSite:Host"], Int32.Parse(ftpconfig["FtpSite:Port"]), ftpconfig["FtpSite:Username"], ftpconfig["FtpSite:Password"]))
            {
                sftp.Connect();
                sftp.ChangeDirectory(ftpconfig["FtpSite:UploadDirectory"]);
                sftp.UploadFile(datastream, fileName);
                sftp.Disconnect();
            }
        }

        // Get last loaded date from ECRTransactionFile
        private static DateTime getLoadDate()
        {
            DateTime lastLoadedDt = new DateTime();
            con.Open();
            // Initialize trsnsaction table
            SqlCommand cmd = new SqlCommand("SELECT TOP 1 CreatedDate FROM ECRTransactionFile", con);
            using (SqlDataReader sdr = cmd.ExecuteReader())
            {
                if (sdr.Read())
                    lastLoadedDt = sdr.GetDateTime(0);
            }
            con.Close();
            return lastLoadedDt;
        }

        // Truncate table and call stored procedure to update database
        private static int updateDatabase(Stream inFile, string ConnectionString, IConfigurationRoot config, bool isFullFile, string ftpFile)
        {
            int transErrorCount = 0;

            DataTable tblcsv = new DataTable();
            //creating columns  
            tblcsv.Columns.Add("TransactionCode");
            tblcsv.Columns.Add("UEID");
            tblcsv.Columns.Add("LastName");
            tblcsv.Columns.Add("FirstName");
            tblcsv.Columns.Add("MiddleName");
            tblcsv.Columns.Add("NameSuffix");
            tblcsv.Columns.Add("DateofBirth");
            tblcsv.Columns.Add("Gender");
            tblcsv.Columns.Add("AddressLine1");
            tblcsv.Columns.Add("AddressLine2");
            tblcsv.Columns.Add("City");
            tblcsv.Columns.Add("State");
            tblcsv.Columns.Add("Zipcode");
            tblcsv.Columns.Add("PhoneNumber");
            tblcsv.Columns.Add("Extension");
            tblcsv.Columns.Add("Ethnicity");
            tblcsv.Columns.Add("AgencyCode");
            tblcsv.Columns.Add("ClassCode");
            tblcsv.Columns.Add("ClassType");
            tblcsv.Columns.Add("BargainingDesignation");
            tblcsv.Columns.Add("BargainingUnit");
            tblcsv.Columns.Add("AppointmentDate");
            tblcsv.Columns.Add("SafetyCode");
            tblcsv.Columns.Add("Tenure");
            tblcsv.Columns.Add("Timebase");
            tblcsv.Columns.Add("ReportingUnit");
            tblcsv.Columns.Add("Serial");
            tblcsv.Columns.Add("CreatedDate", typeof(DateTime));

            //Reading All text from memory 
            inFile.Seek(0, SeekOrigin.Begin);
            using (StreamReader reader = new StreamReader(inFile))
            {
                string ReadCSV = reader.ReadToEnd().ToString();

                //spliting row after new line and add current datetime
                int rowidx = 0;
                foreach (string csvRow in ReadCSV.Split(new char[] { '\r', '\n' }))
                {
                    if (!string.IsNullOrEmpty(csvRow))
                    {
                        //Adding each row into datatable             
                        tblcsv.Rows.Add(Regex.Split(csvRow, "\\|(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"));
                        tblcsv.Rows[rowidx]["CreatedDate"] = DateTime.Now;
                        rowidx++;
                    }
                }
            }

            con.Open();

            // Delete records from ECRTransactionFile table
            SqlCommand cmd = new SqlCommand("Truncate Table [EmployeeCoreRecord].[dbo].[ECRTransactionFile]", con);
            cmd.ExecuteNonQuery();

            // Use SqlBulkCopy to insert employee data
            SqlBulkCopy objbulk = new SqlBulkCopy(con);
            objbulk.DestinationTableName = "ECRTransactionFile";
            objbulk.WriteToServer(tblcsv);

            // Execute stored procedure to perform database update
            cmd = new SqlCommand("pLoadEmployeeInfo", con);
            cmd.CommandType = System.Data.CommandType.StoredProcedure;
            cmd.Parameters.Add("@returnVal", SqlDbType.Char, 2000);
            cmd.Parameters["@returnVal"].Direction = ParameterDirection.Output;
            cmd.CommandTimeout = 1200;
            cmd.ExecuteNonQuery();
            string message = (string)cmd.Parameters["@returnVal"].Value;
            if (!message.TrimEnd().Equals("COMPLETED"))
            {
                throw new Exception(string.Format("ECR Stored Procedure 'pLoadEmployeeInfo' failed.<BR><BR> {0}", message));
            }

            // Execute stored procedure for Full Data Load only. Change recordstatus of UEID that does not exist in transacton file
            if (isFullFile)
            {
                cmd = new SqlCommand("pDeleteEmployee", con);
                cmd.CommandType = System.Data.CommandType.StoredProcedure;
                cmd.ExecuteNonQuery();
            }


            // Create Transaction Error csv file and upload to CalHR.
            List<string> columnList = new List<string> { "TransactionCode"
                ,"UEID", "Gender"
                ,"City","State","ZipCode","Ethnicity","AgencyCode","ClassCode","ClassType","BargainingUnit"
                ,"AppointmentDate","SafetyCode","Tenure","Timebase"
                ,"CONVERT(VARCHAR(11),TenureTimebaseId )"
                ,"CONVERT(VARCHAR(11),GenderId)"
                ,"CONVERT(VARCHAR(11),NameSuffixId)"
                ,"CONVERT(VARCHAR(11),TimebaseId)"
                ,"CONVERT(VARCHAR(11),TenureId)"
                ,"CONVERT(VARCHAR(11),ClassificationId)"
                ,"CONVERT(VARCHAR(11),CollectiveBargainingIdentificationId)"
                ,"CONVERT(VARCHAR(11),FacilityId)"
                ,"CONVERT(VARCHAR(11),SafetyCodeId)"
                ,"CONVERT(VARCHAR(11),EthnicityId)"
                ,"CONVERT(VARCHAR(11),ClassTypeId)"
                ,"ErrorMsg"};
            string columns = string.Join(",", columnList);
            string csvheader = columns.Replace("CONVERT(VARCHAR(11),", ""); ;
            csvheader = csvheader.Replace(")", "");
            string qry = "SELECT " + columns + " FROM ECRTransactionFile WHERE InvalidRecord = 1";
            cmd = new SqlCommand(qry, con);
            cmd.CommandType = CommandType.Text;

            UnicodeEncoding uniEncoding = new UnicodeEncoding();
            using (MemoryStream csvStream = new MemoryStream())
            {
                var sw = new StreamWriter(csvStream, uniEncoding);
                try
                {
                    using (SqlDataReader sdr = cmd.ExecuteReader())
                    {
                        if (sdr.HasRows)
                        {
                            sw.WriteLine(csvheader);
                            string csvline = "";
                            string delimiter = ",";
                            while (sdr.Read())
                            {
                                transErrorCount++;
                                for (int i = 0; i < columnList.Count; i++)
                                {
                                    if (i > 0)
                                    {
                                        if (sdr.IsDBNull(i))
                                            csvline = csvline + delimiter;
                                        else
                                            csvline = csvline + delimiter + sdr.GetString(i);
                                    }
                                    else
                                        csvline = sdr.GetString(0);
                                }
                                sw.WriteLine(csvline);
                            }
                        }
                    }
                    sw.Flush();//otherwise you are risking empty stream
                    csvStream.Seek(0, SeekOrigin.Begin);
                    if (transErrorCount > 0)
                    {

                        Policy
                            .Handle<Exception>()
                            .WaitAndRetry(Int32.Parse(config["FtpSite:Retry"]), retryAttempt => TimeSpan.FromSeconds(Int32.Parse(config["FtpSite:WaitSec"])))
                            .Execute(() => uploadFtpFile(csvStream, config, ftpFile));
                    }

                }
                finally
                {
                    sw.Dispose();
                }
            }
            con.Close();
            return transErrorCount;
        }

        public static void sendMail(string host, int port, MailMessage message)
        {
            SmtpClient smtpClient;
            smtpClient = new SmtpClient(host, port);
            if (host.Equals("localhost"))
                smtpClient.UseDefaultCredentials = false;
            smtpClient.Send(message);

        }

    }
}
