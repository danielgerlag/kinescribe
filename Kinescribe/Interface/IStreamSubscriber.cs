using Amazon.Kinesis.Model;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Kinescribe.Interface
{
    public interface IStreamSubscriber
    {
        Task Subscribe(string appName, string stream, Action<Record> action);
    }
}
