using Hl7.Fhir.Introspection;
using Hl7.Fhir.Model;
using Hl7.Fhir.Utility;
using Newtonsoft.Json;
using ParquetToFirely.ParquetReaders;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.Serialization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

using Expression = System.Linq.Expressions.Expression;

/**
 * TODO(agw): 
 * - deserialize ViewDefinitions
 * - generate ColumnsMeta from ViewDefinitions
 * - wrapper for VD + JSON -> Parquet
 */

namespace ParquetToFirely
{
    ////////////////////
    // Column Metadata
    public class MemberPathPiece
    {
        public string MemberName;
        public int MatchingIndexColumn;

        public Func<object, object?> SingleConstructorAction;
        public Func<IList?> ListConstructorAction;
        public Func<object, object?> GetCurrentValue;
        public Action<object, object> SetValueAction;
        public Type InternalFhirType;

        public MemberPathPiece()
        {
            // stub functions
            ListConstructorAction = () => { return null; };
            SingleConstructorAction = (object obj) => { return null; };
            GetCurrentValue = (object obj) => { return null; };
            SetValueAction = (object obj, object value) => { };

            MemberName = "";
            MatchingIndexColumn = 0;
            InternalFhirType = typeof(void);
        }
    };

    public class ColumnMeta
    {
        public int ColumnIndex;
        public Type CSharpType;
        public MemberPathPiece[] MemberPath = Array.Empty<MemberPathPiece>();
        public string FHIRPath;
        public int MatchingIndexColumn;
    };

    public class ColumnsMeta
    {
        public List<ColumnMeta> Columns;
        public int ResourceKeyColumnIdx;

        public ColumnsMeta(IList<ViewDefinitionColumn> columns, ParquetReader parquetFileReader)
        {
            Columns = new List<ColumnMeta>(); 
            for(int i = 0; i < columns.Count(); i ++)
            {
                var column = columns[i];

                if(column.FhirPath == "getResourceKey()")
                {
                    ResourceKeyColumnIdx = i;
                    column.FhirPath = "id"; // TODO(agw): not sure we want to put this here...only if resulting type contains "id" field
                }

                var columnMeta = new ColumnMeta();
                columnMeta.CSharpType = column.CSharpType;
                columnMeta.ColumnIndex = i;
                columnMeta.FHIRPath = column.FhirPath;

                // split member path
                // TODO(agw): what about function calls or invalid fhir path expressions for parquet?
                var splits = column.FhirPath.Split('.');

                columnMeta.MemberPath = new MemberPathPiece[splits.Length];
                bool isResourceKey = ResourceKeyColumnIdx == i;
                for(int pieceIndex = 0; pieceIndex < splits.Length; pieceIndex++)
                {
                    if (splits[pieceIndex].Contains("(") || splits[pieceIndex].Contains("$"))
                    {
                        // TODO(agw): right now this is an invalid path
                        columnMeta.MemberPath = Array.Empty<MemberPathPiece>();
                        break;
                    }
                    var piece = new MemberPathPiece() { 
                        MemberName = splits[pieceIndex],
                        MatchingIndexColumn = column.MemberPathPieces[pieceIndex].MatchingIndexColumn 
                    };
                    columnMeta.MemberPath[pieceIndex] = piece;
                }

                Columns.Add(columnMeta);
            }


            //var firelyType = def.FirelyType;
            var firelyType = typeof(Encounter);


            // take parquet schema + view definition schema and make "execution table"
            for (int i = 0; i < parquetFileReader.ColumnsCount; i++)
            {
                // find matching column index
                var matchingColumn = Columns.Where(c => c.ColumnIndex == i).FirstOrDefault();
                if (matchingColumn == null) continue;

                // ~ find column type
                Type columnType = typeof(void);
                {
                    (Type physicalType, Type logicalType, Type elementType) = parquetFileReader.GetSystemTypes(ParquetReader.HandleFromColumnIndex(i));

                    columnType = logicalType;

                    // map logicalType to matching column's CSharpType
                    if (logicalType != matchingColumn.CSharpType)
                    {
                        //// handle maybe nullable to not nullable etc.
                        bool assignableFromNullable = Nullable.GetUnderlyingType(logicalType) != null;
                        if (assignableFromNullable && logicalType.GetGenericArguments()[0] == matchingColumn.CSharpType)
                        {
                            matchingColumn.CSharpType = logicalType;
                        }
                        else
                        {
                            throw new Exception("Mismatching Parquet Schema");
                        }
                    }
                }

                // Create constructor for them?
                Type currType = firelyType;
                foreach (MemberPathPiece piece in matchingColumn.MemberPath)
                {
                    IEnumerable<PropertyInfo> propsWithFhirElement = new PropertyInfo[0];
                    // find all fhir properties
                    {
                        var attributes = currType.GetCustomAttributes<FhirElementAttribute>();
                        propsWithFhirElement = currType.GetProperties().Where(p => p.IsDefined(typeof(FhirElementAttribute)));
                    }

                    var matchingProp = propsWithFhirElement.Where(p =>
                    {
                        return p.GetCustomAttribute<FhirElementAttribute>()?.Name == piece.MemberName;
                    }).FirstOrDefault();

                    if (matchingProp == null)
                    {
                        // just have nil fhir member info
                        throw new Exception("error in member path, no matching FHIR element");
                    }

                    var propGetter = ObjectsFromParquet.GenerateProperyGetter(matchingProp);
                    piece.GetCurrentValue = (object obj) =>
                    {
                        return propGetter(obj);
                    };

                    var propSetter = ObjectsFromParquet.GenerateProperySetter(matchingProp);
                    piece.SetValueAction = (object obj, object value) => { propSetter(obj, value); };

                    Type internalType = matchingProp != null ? matchingProp.PropertyType : typeof(void);

                    bool isList = typeof(IList).IsAssignableFrom(matchingProp.PropertyType);
                    if (isList)
                    {
                        var noArgumentConstructors = matchingProp.PropertyType.GetConstructors().Where(c => c.GetParameters().Count() == 0);
                        var listConstructor = noArgumentConstructors.FirstOrDefault();

                        if (listConstructor != null)
                        {
                            piece.ListConstructorAction = () =>
                            {
                                return (IList)listConstructor.Invoke(null);
                            };
                        }

                        internalType = matchingProp.PropertyType.GetGenericArguments().First();
                    }

                    // get member constructor and type
                    {
                        var singleArgumentConstructors = internalType.GetConstructors().Where(c =>
                        {
                            var parameters = c.GetParameters();
                            return parameters.Count() == 1;
                        });

                        var noArgumentConstructors = internalType.GetConstructors().Where(c => c.GetParameters().Count() == 0);

                        if (singleArgumentConstructors.Any())
                        {
                            var singleArgumentConstructor = singleArgumentConstructors.FirstOrDefault()!;
                            var firstParamType = singleArgumentConstructor.GetParameters().First().ParameterType;

                            Func<object, object> singleArgumentConstructorLambda = null;
                            {
                                singleArgumentConstructorLambda = ObjectsFromParquet.SingleArgumentConstructorHelper(singleArgumentConstructor);

                            }

                            // TODO(agw): not sure if there is a way to do this dynamically
                            // perhaps we can try to automatically find a best fit...
                            // cache these functions? will the compile already know to do that?
                            if (singleArgumentConstructor.DeclaringType.IsGenericType && 
                                singleArgumentConstructor.DeclaringType.GetGenericTypeDefinition() == typeof(Code<>))
                            {
                                piece.SingleConstructorAction = (object obj) => {
                                    return obj;
                                };

                                var underlyingNonNullableType = Nullable.GetUnderlyingType(firstParamType);
                                {
                                    var internalCodeType = firstParamType;
                                    if (underlyingNonNullableType != null)
                                    {
                                        internalCodeType = underlyingNonNullableType;
                                    }
                                    var nonGenericCode = typeof(Hl7.Fhir.Model.Code<>).MakeGenericType(new Type[] { internalCodeType });
                                    ConstructorInfo? properTypeConstructor = nonGenericCode.GetConstructors().Where(c => c.GetParameters().Count() == 1).FirstOrDefault();

                                    singleArgumentConstructorLambda =  ObjectsFromParquet.SingleArgumentConstructorHelper(properTypeConstructor);
                                }


                                var setMethod = ObjectsFromParquet.GenerateProperySetter(matchingProp);
                                Dictionary<string, Enum?> enumCache = new();

                                // fill cache
                                if (underlyingNonNullableType.IsEnum)
                                {
                                    var enumFields = underlyingNonNullableType.GetTypeInfo().DeclaredFields.Where(a => a.IsPublic && a.IsStatic);

                                    foreach (FieldInfo item in enumFields)
                                    {
                                        var attribute = item.GetCustomAttribute<EnumLiteralAttribute>();
                                        string text = attribute?.Literal ?? item.Name;
                                        var val = item.GetValue(null);
                                        enumCache.Add(text, (Enum?)val);
                                    }
                                }

                                piece.SetValueAction = (object obj, object value) =>
                                {
                                    Enum? literal = null;
                                    if (enumCache.TryGetValue((string)value, out Enum enumValue))
                                    {
                                        literal = enumValue;
                                    }

                                    var codedEnum = singleArgumentConstructorLambda(literal);
                                    setMethod(obj, codedEnum);
                                };
                            }
                            else if(firstParamType == typeof(Uri))
                            {
                                piece.SingleConstructorAction = (object obj) => {
                                    var furi = new FhirUri();
                                    furi.Value = (string)obj;
                                    return furi;
                                };
                            }
                            else if(firstParamType == typeof(DateTimeOffset) && columnType == typeof(DateTime?))
                            {
                                piece.SingleConstructorAction = (object obj) => {
                                    DateTimeOffset dto = new DateTimeOffset((DateTime)obj);
                                    return singleArgumentConstructorLambda(dto);
                                };
                            }
                            else 
                            {
                                piece.SingleConstructorAction = (object obj) => { return singleArgumentConstructorLambda(obj); };
                            }
                        }
                        else if(noArgumentConstructors.Any())
                        {
                            var noArgumentConstructor = noArgumentConstructors.FirstOrDefault();
                            NewExpression newExpression = System.Linq.Expressions.Expression.New(noArgumentConstructor);
                            var lambda = System.Linq.Expressions.Expression.Lambda<Func<object>>(newExpression);
                            var noArgumentConstructorLambda = lambda.Compile();
                            piece.SingleConstructorAction = (object obj) =>
                            {
                                //return FormatterServices.GetUninitializedObject(internalType);
                                return noArgumentConstructorLambda();
                            };
                        }
                        else
                        {
                            // stub
                            piece.SingleConstructorAction = (object obj) =>
                            {
                                return null;
                            };
                        }
                    }

                    piece.InternalFhirType = internalType;
                    currType = internalType;
                }
            }
        }
    };

    public class ObjectsFromParquet
    {
        private Func<Resource> _firelyResourceConstructor = null;

        public ObjectsFromParquet()
        {
            _firelyResourceConstructor = () => { return null; };
        }

        private void ResourceSetPropertyFromPath(Resource resource, Type resourceType, IList<IReadOnlyList<object>> columnValues, MemberPathPiece[] path, object value, int rowIdx)
        {
            Type currType = resourceType;

            object currObject = resource;
            for (int pathIdx = 0; pathIdx < path.Length; pathIdx++)
            {
                var piece = path[pathIdx];

                var objValue = piece.GetCurrentValue(currObject);

                if (objValue == null)
                {
                    // call constructors
                    objValue = piece.ListConstructorAction();
                }

                bool objectIsList = objValue is IList;
                if (objectIsList)
                {
                    var objAsList = (IList)objValue;

                    int? listIndex = (int?)columnValues[piece.MatchingIndexColumn][rowIdx];

                    // Make more elements if we don't have enough
                    while (objAsList.Count < (listIndex ?? -1) + 1)
                    {
                        objAsList.Add(null);
                    }

                    if (listIndex != null && objAsList[(int)listIndex] == null)
                    {
                        var internalFhirValue = piece.SingleConstructorAction(value);
                        objAsList[(int)listIndex] = internalFhirValue;
                    }

                    if(listIndex != null)
                    {
                        currObject = objAsList[(int)listIndex];
                    }
                    else
                    {
                        break; // stop if we have no more object to work on
                    }
                }
                else if(objValue == null) // leaf/end node
                {
                    var internalValueResult = piece.SingleConstructorAction(value);
                    piece.SetValueAction(currObject, internalValueResult);
                    currObject = internalValueResult;
                }
                else
                {
                    currObject = objValue;
                }

                currType = piece.InternalFhirType;
            }
        }

        public IList<Resource> ResourcesFromViewDefinitionAndParquet(ViewDefinition def, ParquetReader parquetReader)
        {
            var viewDefinitionColumns = Helpers.FlattenViewDefinition(def);
            var columnsMeta = new ColumnsMeta(viewDefinitionColumns, parquetReader);

            var firelyResourceType = typeof(Encounter);

            // make firely resource delegate
            {
                ConstructorInfo? firelyTypeConstructor = firelyResourceType.GetConstructor(Type.EmptyTypes);
                var newExpr = Expression.New(firelyTypeConstructor);
                var lambda = Expression.Lambda(newExpr);
                var firelyResourceNew = (Func<object>)lambda.Compile();
                _firelyResourceConstructor = () => { return firelyResourceNew() as Resource; };
            }

            // TODO(agw): what could be ideal is multiple regular dictionaries (one for each processor)
            // then you join them at the end, resolving any duplicate resources
            //var resources = new Dictionary<object, Resource>();
            var resources = new List<Resource>();

            Type[] cSharpTypes = new Type[parquetReader.ColumnsCount];
            for (int i = 0; i < columnsMeta.Columns.Count(); i++)
            {
                var meta = columnsMeta.Columns[i];
                cSharpTypes[meta.ColumnIndex] = meta.CSharpType;
            }

            foreach(Handle rowGroupHandle in parquetReader.RowGroups())
            {
                // we want to chunk this over many threads
                //var processorCount = Environment.ProcessorCount;
                var processorCount = 1;

                var rowCount = parquetReader.RowGroupRowCount(rowGroupHandle);

                var perProcessor = (int)rowCount / processorCount;
                var leftOver = rowCount % processorCount;
                leftOver = 0;

                List<IReadOnlyList<object>>[] columnChunksArray = new List<IReadOnlyList<object>>[processorCount];

                for(int i = 0; i < processorCount; i++)
                {
                    var countOnProcessor = perProcessor + ((leftOver > 0) ? 1 : 0);
                    leftOver -= 1;


                }


                {
                    var block = new GP.ProfileBlock("Writing Values");

                    var inner = new GP.ProfileBlock("Loading Column");

                    IReadOnlyList<object>[] columnChunks = new IReadOnlyList<object>[parquetReader.ColumnsCount];
                    for (int j = 0; j < parquetReader.ColumnsCount; j++)
                    {
                        var values = parquetReader.GetRowGroupColumnValues(rowGroupHandle, ParquetReader.HandleFromColumnIndex(j), 0);
                        columnChunks[j] = values;
                    }

                    inner.End();

                    var threadIdx = 0;
                    object currentId = "";
                    // here we are going to assume that all encounters are sequential
                    foreach (ColumnMeta columnMeta in columnsMeta.Columns)
                    {
                        var currIdx = 0;
                        if (columnMeta.MemberPath.Length == 0)
                        {
                            continue;
                        }
                        var columnValues = columnChunks[columnMeta.ColumnIndex];

                        // todo(agw): hydrate firely classes
                        int row_idx = -1;
                        foreach (var value in columnValues)
                        {
                            row_idx++;
                            var resourceKey = columnChunks[columnsMeta.ResourceKeyColumnIdx][row_idx];

                            Resource currResource = resources.Count() > currIdx ? resources[currIdx] : null;
                            currentId = currResource?.Id ?? "";

                            if(resourceKey != currentId)
                            {
                                currResource = _firelyResourceConstructor();
                                resources.Add(currResource);
                                currentId = resourceKey;
                            }

                           /* 
                            if (resources.TryGetValue(resourceKey, out Resource cachedResource))
                            {
                                currResource = cachedResource;
                            }
                            else
                            {
                                currResource = _firelyResourceConstructor();

                                // what happens in a race condition when we over-write this?
                                resources[resourceKey] = currResource;
                            }
                           */

                            ResourceSetPropertyFromPath(currResource, firelyResourceType, columnChunks, columnMeta.MemberPath, value, row_idx);
                            currIdx++;
                        }
                    }
                    block.End();
                }
            }

            // TODO(agw): how fast is this?
            return resources;
        }

        // TODO(agw): we will want to be able to use file streams too
        public IList<Resource> ResourcesFromViewDefinitionAndParquet(string viewDefPath, string parquetPath)
        {
            var jsonString = File.ReadAllText(viewDefPath);
            var options = new JsonSerializerOptions
            {
                DictionaryKeyPolicy = JsonNamingPolicy.CamelCase
            };

            var viewDef = JsonConvert.DeserializeObject<ViewDefinition>(jsonString);

            ParquetReader parquetReader = new NativeParquetReader(parquetPath);
            IList<Resource> result = ResourcesFromViewDefinitionAndParquet(viewDef, parquetReader);
            return result;
        }


        ////////////////////
        // Lambda Function Generation Helpers
        public static Func<object, object> SingleArgumentConstructorHelper(ConstructorInfo constInfo)
        {
            MethodInfo genericHelper = typeof(ObjectsFromParquet).GetMethod(nameof(SingleArgumentConstructorMethodHelper), BindingFlags.Static | BindingFlags.NonPublic);

            MethodInfo constructedHelper = genericHelper.MakeGenericMethod(constInfo.GetParameters()[0].ParameterType, constInfo.DeclaringType);

            object ret = constructedHelper.Invoke(null, new object[] {constInfo});

            return (Func<object, object>)ret;
        }

        static Func<object, object> SingleArgumentConstructorMethodHelper<TParam, TReturn>(ConstructorInfo method)
        {
            var param = Expression.Parameter(method.GetParameters()[0].ParameterType);
            var newExpression = Expression.New(method, param);
            var lambda = Expression.Lambda(newExpression, param);
            var compiled = (Func<TParam, TReturn>)lambda.Compile();

            Func<object, object> ret = (object param) => compiled((TParam)param);
            return ret;
        }

        public static Func<object, object> GenerateProperyGetter(PropertyInfo propInfo)
        {
            MethodInfo genericHelper = typeof(ObjectsFromParquet).GetMethod(nameof(GeneratePropertyGetterHelper), BindingFlags.Static | BindingFlags.NonPublic);

            MethodInfo constructedHelper = genericHelper.MakeGenericMethod(propInfo.DeclaringType, propInfo.PropertyType);
            object ret = constructedHelper.Invoke(null, new object[] {propInfo});
            return (Func<object, object>)ret;
        }

        static Func<object, object> GeneratePropertyGetterHelper<TInstance, TReturn>(PropertyInfo propInfo)
        {
            var instanceExpr = Expression.Parameter(propInfo.DeclaringType);
            var propertyExpr = Expression.Property(instanceExpr, propInfo.Name);
            var lambda = Expression.Lambda(propertyExpr, instanceExpr);
            var compiled = (Func<TInstance, TReturn>)lambda.Compile();

            Func<object, object> ret = (object param) => compiled((TInstance)param);
            return ret;
        }

        public static Action<object, object> GenerateProperySetter(PropertyInfo propInfo)
        {
            MethodInfo genericHelper = typeof(ObjectsFromParquet).GetMethod(nameof(GeneratePropertySetterHelper), BindingFlags.Static | BindingFlags.NonPublic);

            MethodInfo constructedHelper = genericHelper.MakeGenericMethod(propInfo.DeclaringType, propInfo.PropertyType);
            object ret = constructedHelper.Invoke(null, new object[] {propInfo});
            return (Action<object, object>)ret;
        }

        static Action<object, object> GeneratePropertySetterHelper<TInstance, TValue>(PropertyInfo propInfo)
        {
            var instanceExpr = Expression.Parameter(typeof(TInstance));
            var propertyExpr = Expression.Property(instanceExpr, propInfo);

            var paramExpr = Expression.Parameter(typeof(TValue));

            var assignExpr = Expression.Assign(propertyExpr, paramExpr);

            var lambda = Expression.Lambda<Action<TInstance, TValue>>(assignExpr, instanceExpr, paramExpr);
            var compiled = lambda.Compile();

            Action<object, object> ret = (object instance, object param) => compiled((TInstance)instance, (TValue)param);
            return ret;
        }
    }

    public class Helpers
    {
        public static Dictionary<string, Type> CSharpTypeFromString = new()
        {
            { "boolean", typeof(bool) },
            { "integer" , typeof(int)},
            { "integer64" , typeof(long)},
            { "unsignedInt" , typeof(int)},
            { "positiveInt", typeof(int) },
            { "time", typeof(DateTime) },
            { "date", typeof(DateTime) },
            { "instant", typeof(DateTime)  },
            {  "dateTime", typeof(DateTime)  },
            { "decimal", typeof(decimal) },
            { "string", typeof(string) },
            { "code", typeof(string) },
            { "id", typeof(string) },
            { "uri" , typeof(string)},
            { "oid", typeof(string) },
            {  "uuid" , typeof(string)},
            { "canonical", typeof(string) },
            { "url", typeof(string) },
            { "markdown", typeof(string) },
            { "base64Binary", typeof(string) },
            { "xhtml", typeof(string) }
        };

        public static Type CSharpTypeFromName(string typeName)
        {
            Type result = typeof(void);

            if (CSharpTypeFromString.TryGetValue(typeName, out Type outType))
            {
                result = outType;
            }

            return result;
        }

        private static IList<MemberPathPiece> MemberPathFromFhirPath(string fhirPath)
        {
            var result = new List<MemberPathPiece>();
            var pieces = fhirPath.Split('.');
            foreach(var piece in pieces)
            {
                var memberPiece = new MemberPathPiece() { MemberName = piece };
                result.Add(memberPiece);
            }

            return result;
        }

        private static void FlattenViewDefinition__SelectImpl(ViewDefinition_Select select, IList<ViewDefinitionColumn> columns, IList<MemberPathPiece> pieces)
        {
            /*
             *  when we have a foreach, we need to add that to the path.
             *  there needs to be a corresponding column
             */
            var hasForEach = select.forEach != null || select.forEachOrNull != null;
            if (hasForEach)
            {
                var pathPiece = new MemberPathPiece();

                // TODO(agw): ensure that forEach is only one member invocation
                pathPiece.MemberName = select.forEach ?? select.forEachOrNull;
                pieces.Add(pathPiece);
            }

            foreach(var column in select.Column)
            {
                if(column.Path.Contains("$index"))
                {
                    var indexColumn = columns.Count();
                    pieces[pieces.Count() - 1].MatchingIndexColumn = indexColumn;
                }

                var viewDefColumn = new ViewDefinitionColumn();
                // fill column
                {
                    var fullPath = "";
                    {
                        var strings = new List<string>();
                        strings.AddRange(pieces.Select(p => p.MemberName));
                        strings.Add(column.Path);
                        fullPath = String.Join('.', strings);
                    }

                    viewDefColumn.FhirPath = fullPath;
                    viewDefColumn.MemberPathPieces = new(pieces);
                    var memberPieces = MemberPathFromFhirPath(column.Path);
                    viewDefColumn.MemberPathPieces.AddRange(memberPieces);
                    
                    viewDefColumn.ColumnName = column.Name;
                    viewDefColumn.CSharpType = typeof(void); 

                     // TODO(agw): do we want this to be a fail if not found?
                     // We should have some validate beforehand which will take care of this
                    if (CSharpTypeFromString.TryGetValue(column.Type, out Type outType))
                    {
                        viewDefColumn.CSharpType = outType;
                    }
                }

                columns.Add(viewDefColumn);
            }

            foreach(var childSelect in select.Select)
            {
                FlattenViewDefinition__SelectImpl(childSelect, columns, pieces);
            }


            // NOTE(agw): we only want to add these columns once.
            if (select.UnionAll.Length > 0)
            {
                FlattenViewDefinition__SelectImpl(select.UnionAll[0], columns, pieces);
            }


            if(hasForEach)
            {
                pieces.RemoveAt(pieces.Count() - 1);
            }
        }

        public static IList<ViewDefinitionColumn> FlattenViewDefinition(ViewDefinition def)
        {
            var result = new List<ViewDefinitionColumn>();
            foreach(var select in def.Select)
            {
                FlattenViewDefinition__SelectImpl(select, result, new List<MemberPathPiece>());
            }
            return result;
        }

    }

    ////////////////////
    // View Definition 
    public class ViewDefinitionColumn
    {
        public string FhirPath;
        public List<MemberPathPiece> MemberPathPieces;
        public Type CSharpType;
        public string ColumnName;
        public int MatchingIndexColumn;
    }

    ////////////////////
    // Raw View Definition 
    public class ViewDefinition_Select
    {
        public string? forEach;
        public string? forEachOrNull;
        public ViewDefinition_Select[] Select = Array.Empty<ViewDefinition_Select>();
        public ViewDefinition_Column[] Column = Array.Empty<ViewDefinition_Column>();
        public ViewDefinition_Select[] UnionAll = Array.Empty<ViewDefinition_Select>();
    };

    public class ViewDefinition_Column
    {
        public string Path;
        public string Name;
        public string? Type;
        public ViewDefinition_Select[] Select = Array.Empty<ViewDefinition_Select>();
    };

    public class ViewDefinition
    {
        public ViewDefinition_Select[] Select = Array.Empty<ViewDefinition_Select>();
        public string? Resource;
    };

}
