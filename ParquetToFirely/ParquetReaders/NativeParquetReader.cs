using Microsoft.CodeAnalysis.CSharp.Syntax;
using ParquetSharp;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace ParquetToFirely.ParquetReaders
{
    internal class NativeParquetReader : ParquetReader, IDisposable
    {
        class RowGroupInfo
        {
            public IReadOnlyList<object>[] ColumnValues;

            public int RowGroupCount;
            public int BufferCount;

            public RowGroupReader reader;
        }

        class ColumnContext
        {
            public LogicalColumnReader logicalReader;
            public Func<int, object> ReadAll;

            ParquetValueType valueType;
            Array valueChunk;
        }

        RowGroupInfo[] _rowGroups;
        ColumnContext[] _columnContexts;

        Type[] _csTypes;

        // ParquetSharp Specific
        MethodInfo? containsNestingMethod = null;
        MethodInfo? createLogicalReaderMethod = null;
        ParquetFileReader nativeFileReader;

        public NativeParquetReader(string fileName, int bufferCount = 4096)
        {
            containsNestingMethod = typeof(LogicalColumnReader).GetMethod("ContainsNestedType", BindingFlags.Static | BindingFlags.NonPublic, new Type[] { typeof(Type) });
            createLogicalReaderMethod = typeof(LogicalColumnReader).GetMethod("Create", BindingFlags.Static | BindingFlags.NonPublic, new Type[] { typeof(ColumnReader), typeof(int), typeof(Type), typeof(bool) });
            if (fileName != null)
            {
                Init(fileName, bufferCount);
            }
        }

        public NativeParquetReader()
            :this(null)
        {
        }

        public void Init(string fileName, int bufferCount)
        {
            nativeFileReader = new ParquetFileReader(fileName);
            ColumnsCount = nativeFileReader.FileMetaData.NumColumns;

            _rowGroups = new RowGroupInfo[nativeFileReader.FileMetaData.NumRowGroups];
            for(int i= 0;i < _rowGroups.Count(); i++)
            {
                _rowGroups[i] = new RowGroupInfo();
                // TODO(agw): make this a long. We will have to update column data fetching to be batched to accomidate for this
                _rowGroups[i].RowGroupCount = (int)nativeFileReader.RowGroup(i).MetaData.NumRows;
                _rowGroups[i].ColumnValues = new IReadOnlyList<object>[ColumnsCount];
                _rowGroups[i].reader = nativeFileReader.RowGroup(i);
                // fill in

                _csTypes = new Type[ColumnsCount];
                for(int j = 0; j < ColumnsCount; j++)
                {
                    (Type physicalType, Type logicalType, Type elementType) = GetSystemTypes(HandleFromColumnIndex(j));
                    _csTypes[j] = logicalType;
                }
            }

            _columnContexts = new ColumnContext[ColumnsCount];
            for(int i = 0; i < ColumnsCount; i++)
            {
                _columnContexts[i] = new ColumnContext();
                // make generic logical reader

                // TODO(agw): for now just set this to zero
                var columnReader = _rowGroups[0].reader.Column(i);

                var csType = _csTypes[i];
                var useNesting = csType != null ? containsNestingMethod.Invoke(null, new object[] {csType}) : false;
                var logicalReader = (LogicalColumnReader)createLogicalReaderMethod.Invoke(null, new object[] { columnReader, bufferCount, csType, useNesting ?? false });
                _columnContexts[i].logicalReader = logicalReader;

                var readAllMethod = logicalReader.GetType().GetMethod("ReadAll", 0, new Type[] { typeof(int) });
                // based on type, create proper buffer
                // create proper lambda to read into proper buffer
                // create lambda to read / convert proper buffer to IReadOnlyList<object>
            }
        }

        public override IReadOnlyList<object> GetRowGroupColumnValues(Handle rowGroupHandle, Handle columnIdx, int count)
        {
            IReadOnlyList<object> result = new object[0];

            if(rowGroupHandle.Index > _rowGroups.Count() || _rowGroups.Count() == 0)
            {
                return new object[0];
            }

            var rowGroup = _rowGroups[rowGroupHandle.Index];

            if(columnIdx.Index > ColumnsCount)
            {
                return new object[0];
            }

            // cache miss
            if (_rowGroups[rowGroupHandle.Index].reader == null)
            {
                _rowGroups[rowGroupHandle.Index].reader = nativeFileReader.RowGroup(rowGroupHandle.Index);
            }


            //var csType = _csTypes[columnIdx.Index];
            //var useNesting = csType != null ? containsNestingMethod.Invoke(null, new object[] {csType}) : false;
            //var logicalReader = createLogicalReaderMethod.Invoke(null, new object[] { columnReader, count, csType, useNesting ?? false });

            // todo(agw): cache this method
            var logicalReader = _columnContexts[columnIdx.Index].logicalReader;

            var readAllMethod = logicalReader.GetType().GetMethod("ReadAll", 0, new Type[] { typeof(int) });

            var readAllResult = readAllMethod.Invoke(_columnContexts[columnIdx.Index].logicalReader, new object[] { _rowGroups[rowGroupHandle.Index].RowGroupCount });

            if(readAllResult is IEnumerable<object> enumerable)
            {
                result = enumerable.ToList().AsReadOnly();
            }
            else if (readAllResult is System.Collections.IEnumerable nonGenericEnumerable)
            {
                result = nonGenericEnumerable.Cast<object>().ToList().AsReadOnly();
            }

            return result;
        }

        public override (Type physicalType, Type logicalType, Type elementType) GetSystemTypes(Handle columnHandle)
        {
            ColumnDescriptor columnDescriptor = nativeFileReader.FileMetaData.Schema.Column(columnHandle.Index);
            (Type physicalType, Type logicalType, Type elementType) = columnDescriptor.GetSystemTypes(new LogicalTypeFactory(), null);
            return (physicalType, logicalType, elementType);
        }

        public void Dispose()
        {
            nativeFileReader.Dispose();
        }

        public override IReadOnlyList<Handle> RowGroups()
        {
            var handles = new List<Handle>(nativeFileReader.FileMetaData.NumRowGroups);
            for(int i =0; i < nativeFileReader.FileMetaData.NumRowGroups; i++)
            {
                handles.Add(new Handle { Index = i });
            }
            return handles;
        }

        public override long RowGroupRowCount(Handle rowGroupHandle)
        {
            long result = nativeFileReader.RowGroup(rowGroupHandle.Index).MetaData.NumRows;
            return result;
        }
    }
}
