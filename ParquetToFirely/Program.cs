using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using Hl7.Fhir.Introspection;
using Hl7.Fhir.Model;
using Hl7.Fhir.Serialization;
using Hl7.Fhir.Utility;
using Hl7.FhirPath;
using Hl7.FhirPath.Sprache;
using ParquetSharp;
using ParquetToFirely.ParquetReaders;
using System.Collections;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.Json;

using Expression = System.Linq.Expressions.Expression;


namespace ParquetToFirely
{
    internal class Program
    {

        ////////////////////
        // Dynamic Functions
        // TODO(agw): these should be buried in the Parquet Reader backend
        public static MethodInfo? containsNestingMethod = null;
        public static MethodInfo? createLogicalReaderMethod = null;
        public static Func<Resource> firelyResourceConstructor = null;

        /*
         *  ViewDefinition => ColumnMetadata
         *  Read parquet with ColumnMetadata => Firely Classes
         * */

        public struct ValueType
        {
            public int a;
        };

        public class ClassType
        {
            public int b;
        }

        static void Main(string[] args)
        {
            //var viewDefInput = File.ReadAllText("encounter_diagnosis.json");
            //var options = new JsonSerializerOptions().ForFhir(ModelInfo.ModelInspector);
            //var patient = JsonSerializer.Deserialize<Patient>(viewDefInput, options);

            //ViewDefinition def = new ViewDefinition();
            //def.FirelyType = typeof(Encounter);
            //def.Select = new List<ViewDefinition_Select>();
            //var topSelect = new ViewDefinition_Select();
            //// fill top select
            //{
            //    def.Select.Add(topSelect);
            //    topSelect.Column = new List<ViewDefinition_Column>();
            //    var colOne = new ViewDefinition_Column();
            //    colOne.Name = "encounter_id";
            //    colOne.Path = "id"; // todo(agw): this need to be getResourceKey()
            //    colOne.Type = "string";
            //}


            var res = BenchmarkRunner.Run<BenchmarkClass>();

            //GP.StartProfile();

            //var resources = new ObjectsFromParquet().ResourcesFromViewDefinitionAndParquet("encounter_definition.json", "output.parquet");

            //GP.StopAndPrintProfile(new StreamWriter(Console.OpenStandardOutput()));

            int idx = -1;

            //foreach (var res in resources)
            //{
            //    idx++;
            //    if (idx > 1) break;

            //    var options = new JsonSerializerOptions().ForFhir(ModelInfo.ModelInspector);
            //    var resString = JsonSerializer.Serialize(res.Value, options);
            //    File.WriteAllText($"res-{res.Key.ToString()}.json", resString);
            //}

        }
    }

}
