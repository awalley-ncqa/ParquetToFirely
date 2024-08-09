using BenchmarkDotNet.Attributes;
using Hl7.Fhir.Model;
using Hl7.Fhir.Serialization;
using ParquetSharp;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

using Expression = System.Linq.Expressions.Expression;

namespace ParquetToFirely
{
    public class BenchmarkClass
    {
        IFhirSerializationEngine serializier = null;
        Encounter[] encounters;

        [GlobalSetup]
        public void GlobalSetup()
        {
            //{
            //    Program.containsNestingMethod = typeof(LogicalColumnReader).GetMethod("ContainsNestedType", BindingFlags.Static | BindingFlags.NonPublic, new Type[] { typeof(Type) });
            //    Program.createLogicalReaderMethod = typeof(LogicalColumnReader).GetMethod("Create", BindingFlags.Static | BindingFlags.NonPublic, new Type[] { typeof(ColumnReader), typeof(int), typeof(Type), typeof(bool) });

            //    ConstructorInfo? firelyTypeConstructor = typeof(Encounter).GetConstructor(Type.EmptyTypes);

            //    var newExpr = System.Linq.Expressions.Expression.New(firelyTypeConstructor);
            //    var lambda = Expression.Lambda(newExpr);
            //    var compiled = (Func<object>)lambda.Compile();

            //    if(firelyTypeConstructor != null)
            //    {
            //        Program.firelyResourceConstructor = () =>
            //        {
            //            return compiled() as Resource;
            //        };
            //    }
            //    else
            //    {
            //        Program.firelyResourceConstructor = () => { return null; };
            //    }
            //}

            var reader = new ParquetFileReader("output.parquet");
            var rg = reader.RowGroup(0);
            encounters = new Encounter[rg.MetaData.NumRows];
            for(int i =0; i < encounters.Length; i++)
            {
                encounters[i] = new Encounter();
            }

            serializier = FhirSerializationEngineFactory.Ostrich(ModelInfo.ModelInspector);
        }

        public void SumRolled()
        {
            int[] arr = new int[1000];

            for (int i = 0; i < arr.Length; i++)
            {
                arr[i] = 10;
            }

            long sum = 0;
            for (int j = 0; j < 10000; j++)
            {
            for(int i = 0; i < arr.Length; i++)
                {
                    sum += arr[i];
                }
            }
        }

        public void SumUnrolled()
        {

            int[] arr = new int[1000];
            for (int i = 0; i < arr.Length; i++)
            {
                arr[i] = 10;
            }

            long sum = 0;
            for (int j = 0; j < 10000; j++)
            {
                for(int i = 0; i < arr.Length; i+=2)
                {
                    sum += arr[i];
                    sum += arr[i+1];
                }
            }

        }

        //[Benchmark]
        public void StrightInvoke()
        {
            var reader = new ParquetFileReader("output.parquet");
            var rg = reader.RowGroup(0);
            var logicalReader = rg.Column(0).LogicalReader<string>();
            var readAllMethod = logicalReader.GetType().GetMethod("ReadAll", 0, new Type[] { typeof(int) });
            var result = readAllMethod.Invoke(logicalReader, new object[] {(int)rg.MetaData.NumRows});
        }

        //[Benchmark]
        public void CompileThenInvoke()
        {
            var reader = new ParquetFileReader("output.parquet");
            var rg = reader.RowGroup(0);
            var logicalReader = rg.Column(0).LogicalReader<string>();
            var readAllMethod = typeof(LogicalColumnReader<string>).GetMethod("ReadAll", 0, new Type[] {  typeof(int) });

            var instanceParam = Expression.Constant(logicalReader);
            var param = System.Linq.Expressions.Expression.Parameter(typeof(int));
            var logicalReaderParam = System.Linq.Expressions.Expression.Parameter(typeof(LogicalColumnReader<string>));
            var callExpr = System.Linq.Expressions.Expression.Call(instanceParam, readAllMethod, param);
            var lambdaExpr = System.Linq.Expressions.Expression.Lambda(callExpr, param);
            var compiled = (Func<int, string[]>)lambdaExpr.Compile();
            
            var result = compiled((int)rg.MetaData.NumRows);
        }

        //[Benchmark]
        public void ReadAll()
        {
            var reader = new ParquetFileReader("output.parquet");
            string res = "";
            int sum = 0;
            for (int i = 0; i < reader.FileMetaData.NumRowGroups; i++)
            {
                var rg = reader.RowGroup(i);
                var encounterIdx = 0;
                var result = rg.Column(0).LogicalReader<string>().ReadAll((int)rg.MetaData.NumRows);
                for(int j = 0; j < result.Length; j ++, encounterIdx++)
                {
                    encounters[encounterIdx].Id = result[j];
                }
            }
        }

        //[Benchmark]
        public void ReadBatched()
        {

            var reader = new ParquetFileReader("output.parquet");
            int res = 0;

            for(int i = 0; i <  reader.FileMetaData.NumRowGroups; i ++)
            {
                var rg = reader.RowGroup(i);
                var batchCount = 16384;
                var logicalReader = rg.Column(0).LogicalReader<string>(batchCount);

                string[] result = new string[batchCount];

                var encounterIdx = 0;

                var numRows = rg.MetaData.NumRows;
                var loc = 0;
                while(numRows > 0)
                {
                    var batchLength = Math.Min((int)numRows, batchCount);
                    logicalReader.ReadBatch(result, 0, batchLength);
                    loc += batchLength;
                    numRows -= batchLength;
                    for (int j = 0; j < batchLength; j++, encounterIdx++)
                    {
                        encounters[encounterIdx].Id = result[j];
                    }
                }
            }

        }


        [Benchmark]
        public void ParquetToEncounters()
        {
            var res = new ObjectsFromParquet().ResourcesFromViewDefinitionAndParquet("encounter_definition.json", "output.parquet");
            //var columnsMeta = Program.TestColumns();
            //using var parquetFile = new ParquetReaders.NativeParquetReader("C:\\Users\\awalley\\Code\\ParquetToFirely\\ParquetToFirely\\output.parquet");

            //Program.ColumnsMetaInit(columnsMeta, typeof(Encounter), parquetFile);
            //var resources = Program.RunParquet(columnsMeta, typeof(Encounter), parquetFile);

            int a = 0;
        }

        [Benchmark]
        public void DeserializeAll()
        {
            // deserialize all
            var files = Directory.EnumerateFiles("C:\\Users\\awalley\\Code\\synthea\\output\\ncqa_fhir\\150k_encounters");
            foreach (var path in files)
            {
                serializier.DeserializeFromJson(File.ReadAllText(path));
            }
        }
    }
}
