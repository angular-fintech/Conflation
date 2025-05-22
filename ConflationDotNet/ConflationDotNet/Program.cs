using System;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using ConflationDotNet;

public static class Program
{
    public static void Main(string[] args)
    {
        Console.WriteLine("C# Conflation .NET");
        Subject<KeyValuePair<string, long>> subject = new Subject<KeyValuePair<string, long>>();

        // Simulate a stream of key value pairs
        var inputStream = Observable.Interval(TimeSpan.FromMilliseconds(100))
            .Select((l) =>
            {
                var data = new KeyValuePair<string, long>(
                    "Key1",
                    l
                );
                Console.WriteLine(data);
                return data;
            });



        inputStream.Subscribe(pair =>
        {
            subject.OnNext(pair);
        });
        
        var inputStream2 = Observable.Interval(TimeSpan.FromMilliseconds(100))
            .Select((l) =>
            {
                var data = new KeyValuePair<string, long>(
                    "Key2",
                    l
                );
                Console.WriteLine(data);
                return data;
            });
        
        
        inputStream2.Subscribe(pair =>
        {
            subject.OnNext(pair);
        });

        // Conflate the stream by key every 1 second
        var conflatedStream = subject.ConflateByKey(TimeSpan.FromSeconds(1));

        // Subscribe to the conflated stream and print the results
        conflatedStream.Subscribe(pair => { Console.WriteLine($"Key: {pair.Key}, Value: {pair.Value}"); });


        // Wait for user input before exiting
        Console.WriteLine("Press any key to exit...");
        Console.ReadKey();
    }


}