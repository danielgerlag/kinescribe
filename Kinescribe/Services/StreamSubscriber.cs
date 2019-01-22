using Amazon;
using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using Amazon.Runtime;
using DynamoLock;
using Kinescribe.Interface;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Kinescribe.Services
{    
    public class StreamSubscriber : IStreamSubscriber, IDisposable
    {
        private readonly ILogger _logger;
        private readonly IShardTracker _tracker;
        private readonly IDistributedLockManager _lockManager;
        private readonly IAmazonKinesis _client;
        private readonly CancellationTokenSource _cancelToken = new CancellationTokenSource();
        private readonly Task _processTask;
        private readonly TimeSpan _snoozeTime = TimeSpan.FromSeconds(3);
        private ICollection<ShardSubscription> _subscribers = new HashSet<ShardSubscription>();

        public StreamSubscriber(AWSCredentials credentials, RegionEndpoint region, IShardTracker tracker, IDistributedLockManager lockManager, ILoggerFactory logFactory)
        {
            _logger = logFactory.CreateLogger(GetType());
            _tracker = tracker;
            _lockManager = lockManager;
            _client = new AmazonKinesisClient(credentials, region);
            _processTask = new Task(Process);
            _processTask.Start();
        }

        public StreamSubscriber(IAmazonKinesis kinesisClient, IShardTracker tracker, IDistributedLockManager lockManager, ILoggerFactory logFactory)
        {
            _logger = logFactory.CreateLogger(GetType());
            _tracker = tracker;
            _lockManager = lockManager;
            _client = kinesisClient;
            _processTask = new Task(Process);
            _processTask.Start();
        }

        //public StreamSubscriber(AWSCredentials credentials, RegionEndpoint region, ILoggerFactory logFactory = NullLoggerFactory.Instance)
        //{
        //    _logger = logFactory.CreateLogger(GetType());
        //    _tracker = new ShardTracker(credentials, region, "kinescribe_shards", logFactory);
        //    _lockManager = new DynamoDbLockManager(credentials, new Amazon.DynamoDBv2.AmazonDynamoDBConfig() { RegionEndpoint = region }, );
        //    _client = new AmazonKinesisClient(credentials, region);
        //    _processTask = new Task(Process);
        //    _processTask.Start();
        //}

        public async Task Subscribe(string appName, string stream, Action<Record> action, int batchSize = 100)
        {
            var shards = await _client.ListShardsAsync(new ListShardsRequest()
            {
                StreamName = stream
            });

            foreach (var shard in shards.Shards)
            {
                _subscribers.Add(new ShardSubscription()
                {
                    AppName = appName,
                    Stream = stream,
                    Shard = shard,
                    Action = action,
                    BatchSize = batchSize
                });
            }
        }

        private async void Process()
        {
            try
            {
                await _lockManager.Start();
            }
            catch (Exception ex)
            {
                _logger.LogError(default(EventId), ex, ex.Message);
            }

            while (!_cancelToken.IsCancellationRequested)
            {
                try
                {
                    var todo = _subscribers.Where(x => x.Snooze < DateTime.Now).ToList();
                    foreach (var sub in todo)
                    {
                        if (!await _lockManager.AcquireLock($"{sub.AppName}.{sub.Stream}.{sub.Shard.ShardId}"))
                            continue;

                        try
                        {
                            var iterator = await _tracker.GetNextShardIterator(sub.AppName, sub.Stream, sub.Shard.ShardId);

                            if (iterator == null)
                            {
                                var iterResp = await _client.GetShardIteratorAsync(new GetShardIteratorRequest()
                                {
                                    ShardId = sub.Shard.ShardId,
                                    StreamName = sub.Stream,
                                    ShardIteratorType = ShardIteratorType.AFTER_SEQUENCE_NUMBER,
                                    StartingSequenceNumber = sub.Shard.SequenceNumberRange.StartingSequenceNumber
                                }, _cancelToken.Token);
                                iterator = iterResp.ShardIterator;
                            }

                            var records = await _client.GetRecordsAsync(new GetRecordsRequest()
                            {
                                ShardIterator = iterator,
                                Limit = sub.BatchSize
                            });

                            if (records.Records.Count == 0)
                                sub.Snooze = DateTime.Now.Add(_snoozeTime);

                            foreach (var rec in records.Records)
                            {
                                try
                                {
                                    sub.Action(rec);
                                }
                                catch (Exception ex)
                                {
                                    _logger.LogError(default(EventId), ex, ex.Message);
                                }
                            }

                            await _tracker.IncrementShardIterator(sub.AppName, sub.Stream, sub.Shard.ShardId, records.NextShardIterator);
                        }
                        finally
                        {
                            await _lockManager.ReleaseLock($"{sub.AppName}.{sub.Stream}.{sub.Shard.ShardId}");
                        }
                    }

                    if (todo.Count == 0)
                        await Task.Delay(_snoozeTime, _cancelToken.Token);
                }
                catch (Exception ex)
                {
                    _logger.LogError(default(EventId), ex, ex.Message);
                }
            }

            await _lockManager.Stop();
        }


        public void Dispose()
        {
            _cancelToken.Cancel();
            _processTask.Wait(5000);
        }

        class ShardSubscription
        {
            public string AppName { get; set; }
            public string Stream { get; set; }
            public Shard Shard { get; set; }
            public Action<Record> Action { get; set; }
            public DateTime Snooze { get; set; } = DateTime.Now;
            public int BatchSize { get; set; }
        }
    }
}
