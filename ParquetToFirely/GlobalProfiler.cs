using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using System.Timers;

namespace ParquetToFirely
{
    internal class GP
    {

        public struct ProfileInfo
        {
            public long TSCElapsedChildren;
            public long TSCElapsedAtRoot;
            public long TSCElapsed;

            public long HitCount;
            public string Label;
        };

        public struct ProfileBlock
        {
            long TSCStart;
            long TSCEnd;
            long OldTSCElapsedAtRoot;

            int ParentIndex;
            int AnchorIndex;

            public ProfileBlock(string label)
            {
                ParentIndex = _currentParent;

                _currentParent += 1;

                AnchorIndex = ++Count;

                OldTSCElapsedAtRoot = anchors[AnchorIndex].TSCElapsedAtRoot;

                anchors[AnchorIndex].Label = label;
                TSCStart = ReadCpuTimer();
            }

            public void End()
            {
                long elapsed = ReadCpuTimer() - TSCStart;
                anchors[ParentIndex].TSCElapsedChildren += elapsed;
                anchors[AnchorIndex].TSCElapsedAtRoot = OldTSCElapsedAtRoot + elapsed;
                anchors[AnchorIndex].TSCElapsed += elapsed;
                anchors[AnchorIndex].HitCount++;

                _currentParent--;
            }
        };

        static ProfileInfo[] anchors = new ProfileInfo[4096];
        static long TSCStart;
        static long TSCEnd;
        static int Count;

        static int _currentParent = 0;
        static int _currentIndex = 0;

        public static void StartProfile()
        {
            TSCStart = ReadCpuTimer();
        }

        public static void StopAndPrintProfile(StreamWriter stream)
        {
            TSCEnd = ReadCpuTimer();
            long totalCpuElapsed = TSCEnd - TSCStart;
            long freq = Stopwatch.Frequency;

            stream.WriteLine($"Total Time: {1000.0 * (double)totalCpuElapsed / (double)freq} ms");

            for (int i = 0; i < anchors.Length; i++)
            {
                var anchor = anchors[i];

                var elapsedSelf = anchor.TSCElapsed - anchor.TSCElapsedChildren;
                if (anchor.Label != null)
                {
                    stream.WriteLine($"{anchor.Label}: [{anchor.HitCount}] {1000.0 * elapsedSelf / freq} ms");
                }
            }

            stream.Flush();
        }

        public static long ReadCpuTimer()
        {
            return Stopwatch.GetTimestamp();
        }
    }
}
