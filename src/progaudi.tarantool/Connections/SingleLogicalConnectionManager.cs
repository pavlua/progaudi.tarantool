using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ProGaudi.Tarantool.Client.Model;
using ProGaudi.Tarantool.Client.Model.Requests;
using ProGaudi.Tarantool.Client.Model.Responses;
using ProGaudi.Tarantool.Client.Utils;

namespace ProGaudi.Tarantool.Client.Connections
{
    public class SingleLogicalConnectionManager : ILogicalConnection
    {
        private readonly ClientOptions _clientOptions;

        private readonly RequestIdCounter _requestIdCounter = new RequestIdCounter();

        private LogicalConnection _droppableLogicalConnection;

        private readonly ManualResetEvent _connected = new ManualResetEvent(true);

        private readonly AutoResetEvent _reconnectAvailable = new AutoResetEvent(true);

        private Timer _timer;

        private int _disposing;

        private const int _connectionTimeout = 1000;

        private const int _pingTimerInterval = 100;

        private readonly int _pingCheckInterval = 1000;

        private readonly TimeSpan? _pingTimeout;

        private DateTimeOffset _nextPingTime = DateTimeOffset.MinValue;

        public SingleLogicalConnectionManager(ClientOptions options)
        {
            _clientOptions = options;
        }

        public uint PingsFailedByTimeoutCount => _droppableLogicalConnection?.PingsFailedByTimeoutCount ?? 0;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposing, 1) > 0)
            {
                return;
            }

            Interlocked.Exchange(ref _droppableLogicalConnection, null)?.Dispose();
            Interlocked.Exchange(ref _timer, null)?.Dispose();
        }

        public async Task Connect()
        {
            if (IsConnectedInternal())
            {
                return;
            }

            if (!_reconnectAvailable.WaitOne(_connectionTimeout))
            {
                throw ExceptionHelper.NotConnected();
            }

            try
            {
                if (IsConnectedInternal())
                {
                    return;
                }

                _connected.Reset();

                _clientOptions.LogWriter?.WriteLine($"{nameof(SingleLogicalConnectionManager)}: Connecting...");

                var singleNode = _clientOptions.ConnectionOptions.Nodes.FirstOrDefault();
                var newConnection = new LogicalConnection(_clientOptions, singleNode, _requestIdCounter);
                await newConnection.Connect().ConfigureAwait(false); ;
                Interlocked.Exchange(ref _droppableLogicalConnection, newConnection)?.Dispose();

                _connected.Set();

                _clientOptions.LogWriter?.WriteLine($"{nameof(SingleLogicalConnectionManager)}: Connected...");
            }
            finally
            {
                _reconnectAvailable.Set();
            }
        }

        public bool IsConnected() => _connected.WaitOne(_connectionTimeout) && IsConnectedInternal();

        private bool IsConnectedInternal()
        {
            return _droppableLogicalConnection?.IsConnected() ?? false;
        }

        public async Task<DataResponse<TResponse[]>> SendRequest<TRequest, TResponse>(TRequest request, TimeSpan? timeout = null)
            where TRequest : IRequest
        {
            await Connect().ConfigureAwait(false);

            return await _droppableLogicalConnection.SendRequest<TRequest, TResponse>(request, timeout).ConfigureAwait(false);
        }

        public async Task<DataResponse> SendRequest<TRequest>(TRequest request, TimeSpan? timeout = null) where TRequest : IRequest
        {
            await Connect().ConfigureAwait(false);

            return await _droppableLogicalConnection.SendRequest(request, timeout).ConfigureAwait(false);
        }

        public async Task<byte[]> SendRawRequest<TRequest>(TRequest request, TimeSpan? timeout = null)
            where TRequest : IRequest
        {
            await Connect().ConfigureAwait(false);

            return await _droppableLogicalConnection.SendRawRequest(request, timeout).ConfigureAwait(false);
        }

        public async Task SendRequestWithEmptyResponse<TRequest>(TRequest request, TimeSpan? timeout = null) where TRequest : IRequest
        {
            await Connect().ConfigureAwait(false);

            await _droppableLogicalConnection.SendRequestWithEmptyResponse(request, timeout).ConfigureAwait(false);
        }
    }
}
