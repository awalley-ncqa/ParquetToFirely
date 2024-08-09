```

BenchmarkDotNet v0.14.0, Windows 10 (10.0.19045.4717/22H2/2022Update)
12th Gen Intel Core i7-12850HX, 1 CPU, 24 logical and 16 physical cores
.NET SDK 8.0.204
  [Host]     : .NET 8.0.4 (8.0.424.16909), X64 RyuJIT AVX2 [AttachedDebugger]
  DefaultJob : .NET 8.0.4 (8.0.424.16909), X64 RyuJIT AVX2


```
| Method              | Mean        | Error     | StdDev    |
|-------------------- |------------:|----------:|----------:|
| ParquetToEncounters |    963.9 ms |  19.18 ms |  20.53 ms |
| DeserializeAll      | 13,208.4 ms | 207.84 ms | 284.49 ms |
