using System.Collections.Concurrent;
using System.Reactive.Linq;

namespace ConflationDotNet;


public static class RxExtensions
{
   public static IObservable<KeyValuePair<TKey, TValue>> ConflateByKey<TKey, TValue>(
      this IObservable<KeyValuePair<TKey, TValue>> source,
      TimeSpan interval)
      where TKey : notnull
   {
      return Observable.Create<KeyValuePair<TKey, TValue>>(
         observer =>
         {
            // Use ConcurrentDictionary for lock-free writes; only swap needs synchronization
            var currentBuffer = new ConcurrentDictionary<TKey, TValue>();
            var isCompleted = 0; // 0 = running, 1 = completed (for lock-free check)
            
            var subscription = source.Subscribe(
               onNext: pair =>
               {
                  // Lock-free write to ConcurrentDictionary.
                  // Race with swap is acceptable: item goes to old or new buffer, 
                  // either way it will be emitted (now or next interval).
                  if (Volatile.Read(ref isCompleted) == 0)
                  {
                     Volatile.Read(ref currentBuffer)[pair.Key] = pair.Value;
                  }
               },
               onError: error =>
               {
                  Interlocked.Exchange(ref isCompleted, 1);
                  observer.OnError(error);
               },
               onCompleted: () =>
               {
                  if (Interlocked.CompareExchange(ref isCompleted, 1, 0) != 0)
                     return; // Already completed
                  
                  // Swap to get final snapshot
                  var finalBuffer = Interlocked.Exchange(ref currentBuffer, new ConcurrentDictionary<TKey, TValue>());
                  
                  foreach (var pair in finalBuffer)
                  {
                     observer.OnNext(pair);
                  }
                  observer.OnCompleted();
               });

            var timer = Observable.Interval(interval).Subscribe(_ =>
            {
               if (Volatile.Read(ref isCompleted) != 0)
                  return;
               
               // Atomically swap to a fresh buffer
               var snapshot = Interlocked.Exchange(ref currentBuffer, new ConcurrentDictionary<TKey, TValue>());
               
               // Skip emission if buffer is empty (avoid enumeration overhead)
               if (snapshot.IsEmpty)
                  return;

               foreach (var pair in snapshot)
               {
                  observer.OnNext(pair);
               }
            });
            
            return () =>
            {
               Interlocked.Exchange(ref isCompleted, 1);
               subscription.Dispose();
               timer.Dispose();
            };
         });
   }

}