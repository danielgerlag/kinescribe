using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Kinescribe.Interface
{
    public interface IShardTracker
    {
        Task<string> GetNextShardIterator(string app, string stream, string shard);
        Task<string> GetNextLastSequenceNumber(string app, string stream, string shard);
        Task IncrementShardIterator(string app, string stream, string shard, string iterator);
        Task IncrementShardIteratorAndSequence(string app, string stream, string shard, string iterator, string sequence);
    }
}
