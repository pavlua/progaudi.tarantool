using ProGaudi.Tarantool.Client.Model;
using ProGaudi.Tarantool.Client.Model.Requests;
using System;
using System.Threading;

namespace ProGaudi.Tarantool.Client
{
    internal class ConnectionPinger
    {
        private Timer _timer;

        private const int _pingTimerInterval = 100;

        private readonly int _pingCheckInterval = 1000;

        private readonly TimeSpan? _pingTimeout;

        private DateTimeOffset _nextPingTime = DateTimeOffset.MinValue;

        private static readonly PingRequest _pingRequest = new PingRequest();

        private int _disposing;

        private ILog _log;

        private ILogicalConnection _conn;

        private TarantoolNode _node;

        public ConnectionPinger(ClientOptions options, ILogicalConnection conn, TarantoolNode node)
        {
            if (options.ConnectionOptions.PingCheckInterval >= 0)
            {
                _pingCheckInterval = options.ConnectionOptions.PingCheckInterval;
            }

            _pingTimeout = options.ConnectionOptions.PingCheckTimeout;
            _log = options.LogWriter;
            _conn = conn;
            _node = node;
        }

        public void Setup()
        {
            if (_pingCheckInterval > 0 && _timer == null)
            {
                _timer = new Timer(x => CheckPing(), null, _pingTimerInterval, Timeout.Infinite);
            }
        }

        public void ScheduleNextPing()
        {
            if (_pingCheckInterval > 0)
            {
                _nextPingTime = DateTimeOffset.UtcNow.AddMilliseconds(_pingCheckInterval);
            }
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposing, 1) > 0)
            {
                return;
            }

            Interlocked.Exchange(ref _timer, null)?.Dispose();
            _conn = null;
        }

        private void CheckPing()
        {
            try
            {
                if (_nextPingTime > DateTimeOffset.UtcNow)
                {
                    return;
                }

                _log?.WriteLine($"{nameof(ConnectionPinger)}: Ping connection {_node.Uri}");
                _conn.SendRequestWithEmptyResponse(_pingRequest, _pingTimeout).GetAwaiter().GetResult();
            }
            catch (Exception e)
            {
                _log?.WriteLine($"{nameof(ConnectionPinger)}: Ping connection {_node.Uri} failed with exception: {e.Message}. Dropping current connection.");
                _conn?.Dispose();
            }
            finally
            {
                if (_disposing == 0)
                {
                    _timer?.Change(_pingTimerInterval, Timeout.Infinite);
                }
            }
        }
    }
}
