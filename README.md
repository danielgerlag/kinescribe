# Kinescribe

Kinescribe is a .Net standard library for processing AWS Kinesis data streams.
It tracks the position in each shard of your stream in DynamoDB so you don't have to worry about it.

```c#
using Kinescribe.Interface;
using Kinescribe.Services;
...

IStreamSubscriber subscriber = new StreamSubscriber();

subscriber.Subscribe("my-app", "my-stream", record =>
{
    using (var reader = new StreamReader(record.Data))
    {
        Console.WriteLine($"Got event {record.SequenceNumber} - {reader.ReadToEnd()}");
    }
});
```

It will create 2 DynamoDB tables:
	`kinesis_shards` 
		This will be used to track each shard iterator per application
	
	`kinesis_locks`
		This will be used as a backing for a distributed lock manager to ensure the same application does not process the same shard at the same time.
