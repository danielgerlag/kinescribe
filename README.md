# Kinescribe

Kinescribe is a .Net standard library for processing AWS Kinesis data streams.
It tracks the position in each shard of your stream in DynamoDB so you don't have to worry about all that extra plumbing.
You can even run multiple instances of the same application, and Kinescribe will coordinate them to distribute the workload and make sure they don't process records in duplicate.

It will create 2 DynamoDB tables:
	`kinesis_shards` 
		This will be used to track each shard iterator per application
	
	`kinesis_locks`
		This will be used as a backing for a distributed lock manager to ensure the same application does not process the same shard at the same time.


## Installing

Using Nuget package console
```
PM> Install-Package Kinescribe
```
Using .NET CLI
```
dotnet add package Kinescribe
```


## Usage

Call the `Subscribe` method on `StreamSubscriber` to pass an action to execute per record on the Kinesis stream.

```c#
using Kinescribe.Interface;
using Kinescribe.Services;
...

var credentials = new EnvironmentVariablesAWSCredentials();
IStreamSubscriber subscriber = new StreamSubscriber(credentials, RegionEndpoint.USWest2, NullLoggerFactory.Instance);

subscriber.Subscribe("my-app", "my-stream", record =>
{
    using (var reader = new StreamReader(record.Data))
    {
        Console.WriteLine($"Got event {record.SequenceNumber} - {reader.ReadToEnd()}");
    }
});
```

## Authors
 * **Daniel Gerlag** - daniel@gerlag.ca

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE.md) file for details
