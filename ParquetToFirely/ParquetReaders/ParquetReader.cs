using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParquetToFirely
{
    public struct Handle
    {
        public int Index;
    };

    public enum ParquetValueType
    {
        Int32,
        NullableInt32,
        Int64,
        NullableInt64,
        Bool,
        NullableBool,
        Float,
        NullableFloat,
        Double,
        NullableDouble,
        DateTime,
        NullableDateTime,
        String,
        NullableString
    };

    public class ParquetValueChunk
    {
        bool[] bools;
        System.Int32[] i32;
        System.Int64[] i64;
        float[] floats;
        double[] doubles;
        byte[] bytes;
        DateTime[] dateTimes;
    };

    public abstract class ParquetReader
    {
        public int ColumnsCount { get; set; }
        public abstract (Type physicalType, Type logicalType, Type elementType) GetSystemTypes(Handle handle);

        public abstract IReadOnlyList<object> GetRowGroupColumnValues(Handle rowGroup, Handle columnHandle, int count);
        //public abstract IReadOnlyList<object> GetRowGroupColumnValuesChunk(Handle rowGroup, Handle columnHandle, int count);
        public abstract IReadOnlyList<Handle> RowGroups();
        public abstract long RowGroupRowCount(Handle rowGroupHandle);

        public static Handle HandleFromColumnIndex(int columnIndex)
        {
            Handle result = new Handle { Index = columnIndex };
            return result;
        }
    }

}
