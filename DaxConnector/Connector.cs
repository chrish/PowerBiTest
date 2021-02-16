using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AnalysisServices.AdomdClient;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Text.RegularExpressions;

namespace DaxConnector
{
    /// <summary>
    /// This is a basic connector class allowing us to connect to a local Analysis Service instance. 
    /// This is launched in the background whenever a Power BI Desktop report is opened, and can be used 
    /// as a basis for testing, at least for a POC. 
    /// </summary>
    public class Connector
    {
        private string _connectionString;
        private string _processName;

        public Connector(string connString)
        {
            // This is the process name of the analysis services executable we're looking 
            // for when trying to find the correct port. 
            _processName = "msmdsrv";

            int id = GetLocalPid();
            int port = GetLocalPort(id);


            _connectionString = connString + ":" + port;
        }

        /// <summary>
        /// Find the pid of the running Analysis Service
        /// </summary>
        /// <returns></returns>
        protected int GetLocalPid()
        {
            var processes = Process.GetProcessesByName(_processName);
            int id = 0;

            if (processes.Length > 0)
            {
                id = processes[0].Id;
            }
            else
            {
                throw new Exception("PID not found");
            }

            return id;
        }

        /// <summary>
        /// Get the port of the local Analysis Services pid. 
        /// In case of problems, ensure that only one instance of 
        /// Power BI Desktop is running. Not sure if it launches 
        /// multiple instances, but this is what I've tested on. 
        /// </summary>
        /// <param name="pid"></param>
        /// <returns></returns>
        protected int GetLocalPort(int pid)
        {

            using (Process p = new Process())
            {
                ProcessStartInfo ps = new ProcessStartInfo();
                ps.Arguments = "-a -n -o";
                ps.FileName = "netstat.exe";
                ps.UseShellExecute = false;
                ps.RedirectStandardOutput = true;
                ps.RedirectStandardError = true;
                ps.RedirectStandardInput = true;

                p.StartInfo = ps;
                p.Start();

                StreamReader stdOut = p.StandardOutput;
                StreamReader stdErr = p.StandardError;

                string output = stdOut.ReadToEnd();
                string error = stdErr.ReadToEnd();

                string exitStatus = p.ExitCode.ToString();

                if (!exitStatus.Equals("0"))
                {
                    throw new Exception("Pid not found or netstat failed. " + error);
                }

                string[] rows = Regex.Split(output, "\r\n");

                foreach (var r in rows)
                {
                    string[] cols = Regex.Split(r, "\\s+");


                    string rowPid = cols[cols.Length - 1];
                    string rowSocket = "";

                    if (cols.Length > 2)
                        rowSocket = cols[2];


                    if (rowPid.Equals(pid.ToString()) && rowSocket.StartsWith("127.0.0.1"))
                    {
                        int port = Convert.ToInt32(rowSocket.Split(':')[1]);

                        return port;
                    }
                }

                return 0;
            }
        }

        /// <summary>
        /// Runs a DAX query against the connected source. 
        /// Unless wrapped in a statement that returns a tabular dataset this will fail. 
        /// </summary>
        /// <param name="query"></param>
        /// <returns></returns>
        public async Task<List<KeyValuePair<string, string>[]>> RunQueryAsync(string query)
        {
            Task<DataTable> results = ExecuteDaxQueryAsync(query);
            List<KeyValuePair<string, string>[]> rows = new List<KeyValuePair<string, string>[]>();

            await results;

            foreach (DataRow row in results.Result.Rows)
            {
                KeyValuePair<string, string>[] r = new KeyValuePair<string, string>[results.Result.Columns.Count];

                for (int i = 0; i < results.Result.Columns.Count; i++)
                {
                    r[i] = new KeyValuePair<string, string>(results.Result.Columns[i].ColumnName, row[i].ToString());
                }
                rows.Add(r);
            }

            return rows;
        }

        /// <summary>
        /// Executes a query that returns a tabular result. 
        /// </summary>
        /// <param name="query"></param>
        /// <returns></returns>
        private async Task<DataTable> ExecuteDaxQueryAsync(string query)
        {
            var tabularResults = new DataTable();

            using (var connection = new AdomdConnection(_connectionString))
            {
                connection.Open();

                var currentDataAdapter = new AdomdDataAdapter(query, connection);


                currentDataAdapter.Fill(tabularResults);
            }

            return tabularResults;
        }

        /// <summary>
        /// This was supposed to be the starting point of the scalar query
        /// execution. Doesnt work, see the measure test class for more info. 
        /// </summary>
        /// <param name="query"></param>
        /// <returns></returns>
        public async Task<string> RunQueryAltAsync(string query)
        {
            var results = ExecuteDaxQueryNonTabularAsync(query);

            return results.Result;
        }

        /// <summary>
        /// Was supposed to execute a scalar query, doesn't work, 
        /// see the measure test class for more info. 
        /// </summary>
        /// <param name="query"></param>
        /// <returns></returns>
        private async Task<string> ExecuteDaxQueryNonTabularAsync(string query)
        {
            string data = "ERROR";

            using (var connection = new AdomdConnection(_connectionString))
            {
                connection.Open();

                AdomdCommand cmd = new AdomdCommand(query);
                cmd.Connection = connection;

                // Doesn't work. Not sure why, but it appears not to be implemented.
                // If run, it will crash upon execute, with no data returned. 
                data = (string)cmd.ExecuteScalar();
            }

            return data;
        }

        /// <summary>
        /// Get a list of all the measures in a model. Will contain Name and DAX
        /// </summary>
        /// <returns></returns>
        public async Task<Dictionary<string, List<Measure>>> GetDaxMeasuresAsync()
        {
            using (var connection = new AdomdConnection(_connectionString))
            {
                connection.Open();

                Dictionary<string, List<Measure>> measures = new Dictionary<string, List<Measure>>();

                foreach (var model in connection.Cubes)
                {
                    if (!measures.ContainsKey(model.Name))
                        measures.Add(model.Name, new List<Measure>());

                    foreach (var measure in model.Measures)
                    {
                        measures[model.Name].Add(measure);
                    }
                }

                return measures;
            }
        }
    }
}
