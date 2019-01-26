using Amazon.DynamoDBv2;
using Kinescribe.Interface;
using Kinescribe.Services;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using Xunit;
using FluentAssertions;
using FakeItEasy;
using Amazon.DynamoDBv2.Model;
using System.Threading;
using System.Collections.Generic;

namespace Kinescribe.Tests
{
    public class ShardTrackerTests
    {
        private IShardTracker _subject;
        private IAmazonDynamoDB _dynamoClient;
        private string _tableName = "locks";

        public ShardTrackerTests()
        {
            _dynamoClient = A.Fake<IAmazonDynamoDB>();
            _subject = new ShardTracker(_dynamoClient, _tableName, NullLoggerFactory.Instance);
        }

        [Fact]
        public async void should_update_shard_iterator()
        {
            await _subject.IncrementShardIterator("app", "stream", "shard", "iter");

            A.CallTo(() => _dynamoClient.UpdateItemAsync(A<UpdateItemRequest>.That.Matches(x => 
                    x.TableName == _tableName && 
                    x.Key["id"].S == "app.stream.shard" &&
                    x.UpdateExpression == "SET next_iterator = :n" &&
                    x.ExpressionAttributeValues[":n"].S == "iter"), A<CancellationToken>.Ignored))
                .MustHaveHappened();
        }

        [Fact]
        public async void should_update_shard_iteratorand_sequence()
        {
            await _subject.IncrementShardIteratorAndSequence("app", "stream", "shard", "iter", "seq");

            A.CallTo(() => _dynamoClient.PutItemAsync(A<PutItemRequest>.That.Matches(x =>
                    x.TableName == _tableName &&
                    x.Item["id"].S == "app.stream.shard" &&
                    x.Item["next_iterator"].S == "iter" &&
                    x.Item["last_sequence"].S == "seq"), A<CancellationToken>.Ignored))
                .MustHaveHappened();
        }

        [Fact]
        public async void should_get_shard_iterator()
        {
            A.CallTo(() => _dynamoClient.GetItemAsync(A<GetItemRequest>.That.Matches(x =>
                    x.TableName == _tableName &&
                    x.Key["id"].S == "app.stream.shard"), A<CancellationToken>.Ignored))
                .Returns(new GetItemResponse()
                {
                    Item = new Dictionary<string, AttributeValue>
                    {
                        { "next_iterator", new AttributeValue("iter") }
                    }
                });

            var result = await _subject.GetNextShardIterator("app", "stream", "shard");

            A.CallTo(() => _dynamoClient.GetItemAsync(A<GetItemRequest>.That.Matches(x =>
                    x.TableName == _tableName &&
                    x.Key["id"].S == "app.stream.shard"), A<CancellationToken>.Ignored))
                .MustHaveHappened();
            result.Should().Be("iter");
        }
    }
}
