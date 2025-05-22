using System.Collections.Concurrent;
using System.Reactive.Linq;

namespace ConflationDotNet;


public static class RxExtensions
{
   public static IObservable<KeyValuePair<TKey, TValue>> ConflateByKey<TKey, TValue>(
      this IObservable<KeyValuePair<TKey, TValue>> source,
      TimeSpan interval)
   {
     return Observable.Create<KeyValuePair<TKey, TValue>>(
        observer =>
        {
           var buffer1 = new ConcurrentDictionary<TKey, TValue>();
           var buffer2 = new ConcurrentDictionary<TKey, TValue>();
           var currentBuffer = buffer1;

            var subscription = source.Subscribe(pair =>
            {
               currentBuffer[pair.Key] = pair.Value;
            });

            var timer = Observable.Interval(interval).Subscribe(_ =>
            {
               // Atomically swap buffers
               var oldBuffer = Interlocked.Exchange(ref currentBuffer, currentBuffer == buffer1 ? buffer2 : buffer1);

               foreach (var pair in oldBuffer)
               {
                  observer.OnNext(pair);
               }

               oldBuffer.Clear();
            });
            
            return () =>
            {
               subscription.Dispose();
               timer.Dispose();
            };

        });
   }

}