import {interval, Observable, Subject} from 'rxjs';


function conflateByKey(source$: Observable<{ key: string, value: any }>, intervalMs: number) {
  return new Observable(observer => {
    const buffer1 = new Map();
    const buffer2 = new Map();
    let currentBuffer = buffer1;

    const sourceSub = source$.subscribe(({ key, value }) => {
      currentBuffer.set(key, value);
    });

    const timerSub = interval(intervalMs).subscribe(() => {
      const oldBuffer = currentBuffer;
      currentBuffer = currentBuffer === buffer1 ? buffer2 : buffer1;

      for (const [key, value] of oldBuffer.entries()) {
        observer.next({ key, value });
      }
      oldBuffer.clear();
    });

    return () => {
      sourceSub.unsubscribe();
      timerSub.unsubscribe();
    };
  });
  }



const subject = new Subject<{ key: string, value: any }>();
const conflated$ = conflateByKey(subject, 1000);

conflated$.subscribe(value => {
  console.log('Conflated value:', value);
})

// Simulate input
setInterval(() =>{
  const item  = Date.now();
  console.log('Input:', item);
  subject.next({ key: 'Key1', value: item });}
  , 100);

