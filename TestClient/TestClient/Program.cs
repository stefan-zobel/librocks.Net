
using librocks.Net;
using System;
using System.Diagnostics;

namespace TestClient;

internal class Program
{
    internal static void Main(string[] args)
    {
        const int MSG_COUNT = 500_000;
        const string family = "Test-DB";

        using var km = new QueueManager("C:/Temp/rocksdb_database");

        Queue kueue = km.Get(family);
        var p = new Producer(kueue, MSG_COUNT);
        var c = new Consumer(kueue, MSG_COUNT);

        var sw = Stopwatch.StartNew();

        c.Start();
        p.Start();

        p.Join();
        c.Join();

        sw.Stop();

        long elapsedMs = sw.ElapsedMilliseconds;

        Console.WriteLine($"put & del took : {elapsedMs} ms");
        Console.WriteLine($"average        : {elapsedMs / (double)MSG_COUNT} ms / message");
        Console.WriteLine($"total puts     : {kueue.TotalPuts}");
        Console.WriteLine($"total takes    : {kueue.TotalTakes}");
        Console.WriteLine($"queue size     : {kueue.Size}");
        Console.WriteLine("done");

//        km.CompactAll();
    }

    class Producer
    {
        private readonly Queue _shared;
        private readonly Random _rnd = new Random();
        private readonly int _max;
        private readonly Thread _thread;

        public Producer(Queue shared, int max)
        {
            _shared = shared;
            _max = max;
            _thread = new Thread(Run);
        }

        public void Start() => _thread.Start();
        public void Join() => _thread.Join();

        private void Run()
        {
            int count = 0;
            while (/*!_shared.IsClosed &&*/ count < _max)
            {
                try
                {
                    _shared.Put(ProduceRandomData(count));
                    ++count;
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.StackTrace);
                    throw;
                }
            }
        }

        byte[] ProduceRandomData(int counter)
        {
            // in Java: nextInt(895) + 1
            int len = _rnd.Next(1, 896);
            int finalLen = len + 8;
            byte[] array = new byte[finalLen];
            int content = _rnd.Next(1, 128);

            Array.Fill(array, (byte)content);
            WriteInt(array, 0, finalLen);
            WriteInt(array, 4, counter);
            return array;
        }
    }

    class Consumer
    {
        private readonly Queue _shared;
        private readonly int _max;
        private readonly Thread _thread;

        public Consumer(Queue shared, int max)
        {
            _shared = shared;
            _max = max;
            _thread = new Thread(Run);
        }

        public void Start() => _thread.Start();
        public void Join() => _thread.Join();

        private void Run()
        {
            int count = 0;
            while (/*!_shared.IsClosed &&*/ count < _max)
            {
                try
                {
                    using NativeBytes bytes = _shared.Take();
                    ++count;
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.StackTrace);
                    throw;
                }
            }
//            Console.WriteLine($"RocksDB version: {_shared.GetKueueManager().GetRocksDBVersion()}");
            Console.WriteLine($"removed        : {count} messages");
        }
    }

    static void WriteInt(byte[] buffer, int offset, int value)
    {
        buffer[offset] = (byte)(value >> 24);
        buffer[offset + 1] = (byte)(value >> 16);
        buffer[offset + 2] = (byte)(value >> 8);
        buffer[offset + 3] = (byte)value;
    }

    static void Main_Old(string[] args)
    {
        using KeyValueStore store = new KeyValueStore("testdb");

        Kind dflt = store.GetDefaultKind();

        Console.WriteLine($"Default Kind Name: {dflt.Name}");

        Span<byte> buffer = stackalloc byte[1024];

        store.Put(dflt, "123"u8, "initialValue"u8);

        if (store.TryUpdateIfPresent(dflt, "123"u8, "abc"u8, buffer, out int written))
        {
            var actualData = buffer.Slice(0, written);

            Console.WriteLine("written: " + written);
            Console.WriteLine($"Value before update: {System.Text.Encoding.UTF8.GetString(actualData)}");
        }

        using NativeBytes bytes = store.UpdateIfPresent(dflt, "123"u8, "juggernaut"u8);
        ReadOnlySpan<byte> before = bytes.Span;
        Console.WriteLine(System.Text.Encoding.UTF8.GetString(before));

        store.GetOrCreateKind("newKind");
        store.GetOrCreateKind("anotherKind");

        var kinds = store.GetKinds();
        foreach (var kind in kinds)
        {
            Console.WriteLine($"Kind Name: {kind.Name}");
        }
        store.Dispose();

        using var qm = new QueueManager("testQueue");
        Queue q = qm.Get("blabla");

        for (int i = 0; i <= 10; i++)
        {
            String s = $"Test {i}";
            q.Put(System.Text.Encoding.UTF8.GetBytes(s));
            Console.WriteLine(q.Take(TimeSpan.FromSeconds(1)));
        }
    }
}
