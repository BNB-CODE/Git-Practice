using Microsoft.Spark.Sql;
using System;
using static Microsoft.Spark.Sql.Functions;
namespace BatchProcessingSparkApp
{
    class Program
    {
        static void Main(string[] args)
        {
            //1. Create a Spark session
            SparkSession spark = SparkSession
                .Builder()
                .AppName("word_count_sample")
                .GetOrCreate();
            //2. Create initial DataFrame
            DataFrame dataFrame = spark.Read()
                //.Schema("Assertid STRING,properties STRING,Value BOOLEAN,TimeSatmp TIMESTAMP")
                .Schema("Assertid STRING,properties STRING,Value STRING,TimeSatmp TIMESTAMP")
                .Csv("DataBook.csv");

            dataFrame.Show();

            //Drop any rows with Null/Empty values
            DataFrameNaFunctions dropEmptytablesrows = dataFrame.Na();
            DataFrame CleanedProjects = dropEmptytablesrows.Drop("any");

            //remove unnecessary Columns
           CleanedProjects = CleanedProjects.Drop("Assertid", "properties", "Value", "TimeSatmp");
           CleanedProjects.Show();
            // Stop Spark session
            //spark.Stop();
        }
    }
}
