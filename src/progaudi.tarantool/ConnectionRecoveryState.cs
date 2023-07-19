using ProGaudi.Tarantool.Client.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ProGaudi.Tarantool.Client
{
    internal class ConnectionRecoveryState
    {
        public int AttemptsCount { get { return attemptsCount; } }
        public TarantoolNode Node { get; set; }

        public void IncrementAttempts()
        {
            Interlocked.Increment(ref attemptsCount);
        }

        private int attemptsCount = 1;
    }
}
