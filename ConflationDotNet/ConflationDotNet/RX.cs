using System.Collections.Concurrent;
using System.Reactive.Linq;

namespace ConflationDotNet;

public static class RxExtensions
{
   /// <summary>
   /// Conflates a stream of key-value pairs by key over fixed time intervals.
   /// Within each interval only the latest value per key is retained.
   /// When the interval elapses the accumulated snapshot is emitted downstream.
   /// </summary>
   /// <remarks>
   /// <para><b>Thread safety:</b> Producers (OnNext) write lock-free to a
   /// <see cref="ConcurrentDictionary{TKey,TValue}"/>.  The periodic drain
   /// and all terminal notifications are serialised by a monitor gate,
   /// satisfying the Rx serialisation contract without ever contending with
   /// producers on the hot path.</para>
   /// <para><b>Memory:</b> Two dictionaries are pre-allocated and reused for
   /// the lifetime of the subscription — zero allocations during steady-state
   /// operation.</para>
   /// </remarks>
   public static IObservable<KeyValuePair<TKey, TValue>> ConflateByKey<TKey, TValue>(
      this IObservable<KeyValuePair<TKey, TValue>> source,
      TimeSpan interval)
      where TKey : notnull
   {
      return Observable.Create<KeyValuePair<TKey, TValue>>(
         observer =>
         {
            // ── Dual-buffer layout ──────────────────────────────────────
            // Two ConcurrentDictionaries are pre-allocated and reused for
            // the entire subscription lifetime (zero steady-state allocs).
            // Producers write to buffers[activeIndex].  The timer swaps
            // activeIndex under the gate, drains the old buffer, clears it,
            // and it becomes available for the next cycle.
            var buffers = new[]
            {
               new ConcurrentDictionary<TKey, TValue>(),
               new ConcurrentDictionary<TKey, TValue>()
            };
            var activeIndex = 0;

            // Gate serialises drain / completion / error / disposal.
            // Producers (onNext) never acquire this — hot path is lock-free.
            // Monitor is reentrant, so downstream operators that dispose us
            // from within OnNext (e.g. Take) will not deadlock.
            var gate = new object();
            var stopped = false; // only mutated under gate

            // ── Hot path: lock-free writes ──────────────────────────────
            var subscription = source.Subscribe(
               onNext: pair =>
               {
                  // Volatile.Read pairs with Volatile.Write inside the gate
                  // (+ Monitor.Exit release fence) to guarantee visibility
                  // of the latest activeIndex without taking a lock.
                  //
                  // If a swap races between reading the index and the dict
                  // write, the item lands in the buffer being drained.
                  // ConcurrentDictionary is safe for concurrent read+write
                  // so no corruption occurs.  The item is either picked up
                  // by the current drain or cleared and superseded by a
                  // future update — expected for key-based conflation.
                  buffers[Volatile.Read(ref activeIndex)][pair.Key] = pair.Value;
               },
               onError: error =>
               {
                  // Acquire gate so any in-progress drain finishes first,
                  // preserving the Rx serialisation contract.
                  lock (gate)
                  {
                     if (stopped) return;
                     stopped = true;
                  }
                  // Terminal notification OUTSIDE the gate so that if
                  // downstream disposes us we don't self-deadlock on a
                  // non-reentrant code path.
                  observer.OnError(error);
               },
               onCompleted: () =>
               {
                  lock (gate)
                  {
                     if (stopped) return;
                     stopped = true;

                     // Swap so any late-arriving producer writes go to the
                     // other buffer (harmless — we are stopping anyway).
                     var drainIdx = activeIndex;
                     Volatile.Write(ref activeIndex, 1 - drainIdx);

                     var buf = buffers[drainIdx];
                     if (!buf.IsEmpty)
                     {
                        foreach (var pair in buf)
                           observer.OnNext(pair);
                        buf.Clear();
                     }
                  }
                  // OnCompleted outside the gate: AutoDetachObserver calls
                  // Dispose on completion, which re-acquires gate — safe
                  // because we already released it above.
                  observer.OnCompleted();
               });

            // ── Cold path: periodic drain ────────────────────────────────
            var timer = Observable.Interval(interval).Subscribe(_ =>
            {
               lock (gate)
               {
                  if (stopped) return;

                  // Swap active index so new producer writes go to the
                  // other buffer while we drain this one.
                  var drainIdx = activeIndex;
                  Volatile.Write(ref activeIndex, 1 - drainIdx);

                  var buf = buffers[drainIdx];
                  if (buf.IsEmpty) return;

                  foreach (var pair in buf)
                  {
                     // A downstream operator (e.g. Take) may dispose us
                     // from within OnNext, which re-enters the gate and
                     // sets stopped = true.  Honour it immediately.
                     if (stopped) break;
                     observer.OnNext(pair);
                  }

                  // Clear for reuse — internal tables are retained so
                  // subsequent writes avoid re-allocation.
                  buf.Clear();
               }
            });

            return () =>
            {
               lock (gate)
               {
                  stopped = true;
               }
               // Dispose outside gate to avoid lock-ordering issues
               // with scheduler internals.
               subscription.Dispose();
               timer.Dispose();
            };
         });
   }
}