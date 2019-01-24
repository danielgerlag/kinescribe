using Kinescribe.Interface;
using Kinescribe.Services;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using Xunit;
using FluentAssertions;
using FakeItEasy;
using System.Threading;
using System.Collections.Generic;
using DynamoLock;
using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using System.Threading.Tasks;

namespace Kinescribe.Tests
{
    public class StreamSubscriberTests
    {
        private IStreamSubscriber _subject;
        private IShardTracker _tracker;
        private IDistributedLockManager _lockManager;
        private IAmazonKinesis _kinesisClient;
        private TimeSpan _waitTime = TimeSpan.FromSeconds(10);

        public StreamSubscriberTests()
        {
            _kinesisClient = A.Fake<IAmazonKinesis>();
            _tracker = A.Fake<IShardTracker>();
            _lockManager = A.Fake<IDistributedLockManager>();
            _subject = new StreamSubscriber(_kinesisClient, _tracker, _lockManager, NullLoggerFactory.Instance);
        }

        [Fact]
        public async void should_get_records()
        {
            //arrange
            var iterator = Guid.NewGuid().ToString();
            var nextIterator = Guid.NewGuid().ToString();
            var recievedSequenceNumber = string.Empty;
            var shardId = Guid.NewGuid().ToString();
            var record = new Amazon.Kinesis.Model.Record
            {
                SequenceNumber = Guid.NewGuid().ToString()
            };

            A.CallTo(() => _kinesisClient.ListShardsAsync(A<ListShardsRequest>.That.Matches(x => x.StreamName == "stream"), A<CancellationToken>.Ignored))
                .Returns(new ListShardsResponse()
                {
                    Shards = new List<Shard>()
                    {
                        new Shard()
                        {
                            ShardId = shardId
                        }
                    }
                });

            A.CallTo(() => _kinesisClient.GetRecordsAsync(A<GetRecordsRequest>.That.Matches(x => x.ShardIterator == iterator), A<CancellationToken>.Ignored))
                .Returns(new GetRecordsResponse()
                {
                    NextShardIterator = nextIterator,
                    Records = new List<Amazon.Kinesis.Model.Record>()
                    {
                        record
                    }
                });

            A.CallTo(() => _tracker.GetNextShardIterator("app", "stream", shardId))
                .Returns(iterator);

            A.CallTo(() => _lockManager.AcquireLock(A<string>.Ignored))
                .Returns(true);

            //act
            await _subject.Subscribe("app", "stream", x => recievedSequenceNumber = x.SequenceNumber);
            await Task.Delay(_waitTime);

            //assert
            A.CallTo(() => _kinesisClient.GetRecordsAsync(A<GetRecordsRequest>.That.Matches(x => x.ShardIterator == iterator), A<CancellationToken>.Ignored))
                .MustHaveHappened();

            recievedSequenceNumber.Should().Be(record.SequenceNumber);
            A.CallTo(() => _tracker.IncrementShardIteratorAndSequence("app", "stream", shardId, nextIterator, record.SequenceNumber)).MustHaveHappened();
            A.CallTo(() => _lockManager.AcquireLock(A<string>.Ignored)).MustHaveHappened();
            A.CallTo(() => _lockManager.ReleaseLock(A<string>.Ignored)).MustHaveHappened();
        }

        [Fact]
        public async void should_update_shard_iterator()
        {
            //arrange
            var iterator = Guid.NewGuid().ToString();
            var nextIterator = Guid.NewGuid().ToString();
            var shardId = Guid.NewGuid().ToString();

            A.CallTo(() => _kinesisClient.ListShardsAsync(A<ListShardsRequest>.That.Matches(x => x.StreamName == "stream"), A<CancellationToken>.Ignored))
                .Returns(new ListShardsResponse()
                {
                    Shards = new List<Shard>()
                    {
                        new Shard()
                        {
                            ShardId = shardId
                        }
                    }
                });

            A.CallTo(() => _kinesisClient.GetRecordsAsync(A<GetRecordsRequest>.That.Matches(x => x.ShardIterator == iterator), A<CancellationToken>.Ignored))
                .Returns(new GetRecordsResponse()
                {
                    NextShardIterator = nextIterator,
                    Records = new List<Amazon.Kinesis.Model.Record>()
                });

            A.CallTo(() => _tracker.GetNextShardIterator("app", "stream", shardId))
                .Returns(iterator);

            A.CallTo(() => _lockManager.AcquireLock(A<string>.Ignored))
                .Returns(true);

            //act
            await _subject.Subscribe("app", "stream", x => { });
            await Task.Delay(_waitTime);

            //assert
            A.CallTo(() => _tracker.IncrementShardIterator("app", "stream", shardId, nextIterator)).MustHaveHappened();
        }

        [Fact]
        public async void should_get_starting_shard_iterator_when_not_yet_tracked()
        {
            //arrange
            var startIterator = Guid.NewGuid().ToString();
            var nextIterator = Guid.NewGuid().ToString();
            var shardId = Guid.NewGuid().ToString();
            var startSeqNum = Guid.NewGuid().ToString();

            A.CallTo(() => _kinesisClient.ListShardsAsync(A<ListShardsRequest>.That.Matches(x => x.StreamName == "stream"), A<CancellationToken>.Ignored))
                .Returns(new ListShardsResponse()
                {
                    Shards = new List<Shard>()
                    {
                        new Shard()
                        {
                            ShardId = shardId,
                            SequenceNumberRange = new SequenceNumberRange()
                            {
                                StartingSequenceNumber = startSeqNum
                            }
                        }
                    }
                });

            A.CallTo(() => _kinesisClient.GetRecordsAsync(A<GetRecordsRequest>.Ignored, A<CancellationToken>.Ignored))
                .Returns(new GetRecordsResponse()
                {
                    NextShardIterator = nextIterator,
                    Records = new List<Amazon.Kinesis.Model.Record>()
                });

            A.CallTo(() => _tracker.GetNextShardIterator("app", "stream", shardId))
                .Returns(Task.FromResult<string>(null));

            A.CallTo(() => _lockManager.AcquireLock(A<string>.Ignored))
                .Returns(true);

            A.CallTo(() => _kinesisClient.GetShardIteratorAsync(A<GetShardIteratorRequest>.That.Matches(x =>
                    x.ShardId == shardId &&
                    x.StreamName == "stream" &&
                    x.StartingSequenceNumber == startSeqNum &&
                    x.ShardIteratorType == ShardIteratorType.AT_SEQUENCE_NUMBER
                ), A<CancellationToken>.Ignored))
                .Returns(new GetShardIteratorResponse()
                {
                    ShardIterator = startIterator
                });

            //act
            await _subject.Subscribe("app", "stream", x => { });
            await Task.Delay(_waitTime);

            //assert
            A.CallTo(() => _kinesisClient.GetShardIteratorAsync(A<GetShardIteratorRequest>.That.Matches(x =>
                    x.ShardId == shardId &&
                    x.StreamName == "stream" &&
                    x.StartingSequenceNumber == startSeqNum &&
                    x.ShardIteratorType == ShardIteratorType.AT_SEQUENCE_NUMBER
                ), A<CancellationToken>.Ignored))
            .MustHaveHappened();

            A.CallTo(() => _kinesisClient.GetRecordsAsync(A<GetRecordsRequest>.That.Matches(x => x.ShardIterator == startIterator), A<CancellationToken>.Ignored))
                .MustHaveHappened();
        }

        [Fact]
        public async void should_lock_shard_id()
        {
            //arrange
            var iterator = Guid.NewGuid().ToString();
            var nextIterator = Guid.NewGuid().ToString();
            var shardId = Guid.NewGuid().ToString();

            A.CallTo(() => _kinesisClient.ListShardsAsync(A<ListShardsRequest>.That.Matches(x => x.StreamName == "stream"), A<CancellationToken>.Ignored))
                .Returns(new ListShardsResponse()
                {
                    Shards = new List<Shard>()
                    {
                        new Shard()
                        {
                            ShardId = shardId
                        }
                    }
                });

            A.CallTo(() => _kinesisClient.GetRecordsAsync(A<GetRecordsRequest>.That.Matches(x => x.ShardIterator == iterator), A<CancellationToken>.Ignored))
                .Returns(new GetRecordsResponse()
                {
                    NextShardIterator = nextIterator,
                    Records = new List<Amazon.Kinesis.Model.Record>()
                });

            A.CallTo(() => _tracker.GetNextShardIterator("app", "stream", shardId))
                .Returns(iterator);

            A.CallTo(() => _lockManager.AcquireLock(A<string>.Ignored))
                .Returns(true);

            //act
            await _subject.Subscribe("app", "stream", x => { });
            await Task.Delay(_waitTime);

            //assert
            A.CallTo(() => _lockManager.AcquireLock($"app.stream.{shardId}")).MustHaveHappened();
            A.CallTo(() => _lockManager.ReleaseLock($"app.stream.{shardId}")).MustHaveHappened();
        }

        [Fact]
        public async void should_get_new_iterator_when_expired()
        {
            //arrange
            var expiredIterator = Guid.NewGuid().ToString();
            var newIterator = Guid.NewGuid().ToString();
            var nextIterator = Guid.NewGuid().ToString();
            var shardId = Guid.NewGuid().ToString();
            var startSeqNum = Guid.NewGuid().ToString();
            var lastSeqNum = Guid.NewGuid().ToString();

            A.CallTo(() => _kinesisClient.ListShardsAsync(A<ListShardsRequest>.That.Matches(x => x.StreamName == "stream"), A<CancellationToken>.Ignored))
                .Returns(new ListShardsResponse()
                {
                    Shards = new List<Shard>()
                    {
                        new Shard()
                        {
                            ShardId = shardId,
                            SequenceNumberRange = new SequenceNumberRange()
                            {
                                StartingSequenceNumber = startSeqNum
                            }
                        }
                    }
                });

            A.CallTo(() => _kinesisClient.GetRecordsAsync(A<GetRecordsRequest>.That.Matches(x =>
                    x.ShardIterator == expiredIterator), A<CancellationToken>.Ignored))
                .Throws(new ExpiredIteratorException(string.Empty));

            A.CallTo(() => _kinesisClient.GetRecordsAsync(A<GetRecordsRequest>.That.Matches(x =>
                    x.ShardIterator == newIterator), A<CancellationToken>.Ignored))
                .Returns(new GetRecordsResponse()
                {
                    NextShardIterator = nextIterator,
                    Records = new List<Amazon.Kinesis.Model.Record>()
                });

            A.CallTo(() => _tracker.GetNextShardIterator("app", "stream", shardId))
                .Returns(Task.FromResult(expiredIterator));

            A.CallTo(() => _tracker.GetLastSequenceNumber("app", "stream", shardId))
                .Returns(Task.FromResult(lastSeqNum));

            A.CallTo(() => _lockManager.AcquireLock(A<string>.Ignored))
                .Returns(true);

            A.CallTo(() => _kinesisClient.GetShardIteratorAsync(A<GetShardIteratorRequest>.That.Matches(x =>
                    x.ShardId == shardId &&
                    x.StreamName == "stream" &&
                    x.StartingSequenceNumber == lastSeqNum &&
                    x.ShardIteratorType == ShardIteratorType.AFTER_SEQUENCE_NUMBER
                ), A<CancellationToken>.Ignored))
                .Returns(new GetShardIteratorResponse()
                {
                    ShardIterator = newIterator
                });

            //act
            await _subject.Subscribe("app", "stream", x => { });
            await Task.Delay(_waitTime);

            //assert
            A.CallTo(() => _kinesisClient.GetShardIteratorAsync(A<GetShardIteratorRequest>.That.Matches(x =>
                    x.ShardId == shardId &&
                    x.StreamName == "stream" &&
                    x.StartingSequenceNumber == lastSeqNum &&
                    x.ShardIteratorType == ShardIteratorType.AFTER_SEQUENCE_NUMBER
                ), A<CancellationToken>.Ignored))
            .MustHaveHappened();

            A.CallTo(() => _kinesisClient.GetRecordsAsync(A<GetRecordsRequest>.That.Matches(x => x.ShardIterator == newIterator), A<CancellationToken>.Ignored))
                .MustHaveHappened();
        }

    }
}
