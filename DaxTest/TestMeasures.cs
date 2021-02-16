using DaxConnector;
using Microsoft.AnalysisServices.AdomdClient;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DaxTest
{
    [TestClass]
    public class TestMeasures
    {
        string _connectionString;

        public TestMeasures()
        {
            // Note: The source runs on a random port when testing this locally. This is handled by the Connector.cs class automatically. 
            // This has only been tested with one instance of Power BI Desktop open, so if in doubt, close all instances except for the 
            // one with the sample model. 
            _connectionString = "DataSource=localhost";
        }

        /// <summary>
        /// The model has three measures; SumItems, NumItems and AltCount.
        /// We're extracting all of them, but only testing the first two. 
        /// 
        /// Please note that ADOMD.NET documentation is kind of incomplete. Based on the interfaces it implements we'd expect 
        /// AdomdCommand.ExecuteScalar to be available and suitable for measures. But for whatever reason any DAX executed using 
        /// this method gives us an error saying "Specified method is not supported", so it would seem that this doesn't work.
        /// 
        /// As a workaround we need to wrap the measure DAX in an evaluate(row()), this at least seem to work in this POC if we 
        /// assume that the first row and only column contains the measure result. 
        /// 
        /// The purpose of this class is to demonstrate that it is possible to extract all the measures in a model to avoid having to 
        /// maintain a list of measures outside the model for testing purposes. Once extracted, the measures are executed and compared 
        /// to the result of calculating the measure in C#. This allows us to verify that the DAX gives us the same result as the C# code, 
        /// making it easy to spot breaking changes. 
        /// </summary>
        [TestMethod]
        public void TestExtractedMeasures()
        {
            var conn = new Connector(_connectionString);
            var measures = conn.GetDaxMeasuresAsync();

            measures.Wait();

            Dictionary<string, string> extractedMeasures = new Dictionary<string, string>();

            // Get the measures from the model
            foreach (KeyValuePair<string, List<Measure>> f in measures.Result)
            {
                foreach (var m in f.Value)
                {
                    if (extractedMeasures.ContainsKey(m.Name))
                    {
                        extractedMeasures.Add(m.Name, m.Expression);
                    }
                    else
                    {
                        extractedMeasures[m.Name] = m.Expression;
                    }
                }
            }

            // Do we have any measures at all from the model? 
            Assert.IsTrue(extractedMeasures.Count > 0);

            // Run through the extracted measures here, execute them and compare with the C# version.
            foreach (var k in extractedMeasures)
            {
                string daxBase = @"evaluate(row(""measure_result"", __MEASURE__))";
                string query = daxBase.Replace("__MEASURE__", k.Value);

                Task<List<KeyValuePair<string, string>[]>> measureData = conn.RunQueryAsync(query);
                measureData.Wait();
                
                // Get the result of the measure. Not a great method, but it seems to work.
                string measureValue = measureData.Result.First()[0].Value;

                // This can be done smoother, but it works for the POC. 
                switch (k.Key)
                {
                    case ("NumItems"):
                        string numItemsCalculatedInTest = GetNumItemsHelper();
                        Assert.AreEqual(measureValue, numItemsCalculatedInTest);
                        break;
                    case ("SumItems"):
                        string sumItemsCalculatedInTest = GetSumItemsHelper();
                        Assert.AreEqual(measureValue, sumItemsCalculatedInTest);
                        break;
                }
            }
        }

        #region DataHelpers

        /// <summary>
        /// Gets the number of items for the measure test. 
        /// </summary>
        /// <returns></returns>
        protected string GetNumItemsHelper()
        {
            string query = $@"EVALUATE(VALUES('fact_data'))";
            var conn = new Connector(_connectionString);
            var data = conn.RunQueryAsync(query);
            data.Wait();

            return data.Result.Count.ToString();
        }

        /// <summary>
        /// Gets the sum of all itemValues for the measure test.
        /// </summary>
        /// <returns></returns>
        protected string GetSumItemsHelper()
        {
            string query = $@"EVALUATE(VALUES('fact_data'))";
            var conn = new Connector(_connectionString);
            var data = conn.RunQueryAsync(query);
            data.Wait();

            int sumItems = 0;

            foreach(var row in data.Result)
            {
                int intVal = Convert.ToInt32(row[1].Value);
                sumItems += intVal;
            }

            return sumItems.ToString();
        }

        #endregion 
    }
}
