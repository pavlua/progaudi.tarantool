using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ProGaudi.Tarantool.Client.Model;
using ProGaudi.Tarantool.Client.Model.Requests;
using ProGaudi.Tarantool.Client.Model.Responses;
using ProGaudi.Tarantool.Client.Utils;

namespace ProGaudi.Tarantool.Client.Connections
{
    public class RoundRobinLogicalConnectionManager : ILogicalConnection
    {
        private readonly ClientOptions _clientOptions;

        private readonly RequestIdCounter _requestIdCounter = new RequestIdCounter();

        private readonly Dictionary<TarantoolNode, SemaphoreSlim> _nodeConnectionSemaphores = new Dictionary<TarantoolNode, SemaphoreSlim>();

        private readonly Dictionary<TarantoolNode, LogicalConnection> _droppableNodeLogicalConnections = new Dictionary<TarantoolNode, LogicalConnection>();


        

        private int _disposing;

        private const int _connectionTimeout = 1000;


        public RoundRobinLogicalConnectionManager(ClientOptions options)
        {
            _clientOptions = options;

            //if (_clientOptions.ConnectionOptions.PingCheckInterval >= 0)
            //{
            //    _pingCheckInterval = _clientOptions.ConnectionOptions.PingCheckInterval;
            //}

            //_pingTimeout = _clientOptions.ConnectionOptions.PingCheckTimeout;


            _nodeConnectionSemaphores = _clientOptions.ConnectionOptions.Nodes.ToDictionary(n => n, _ => new SemaphoreSlim(1, 1));
        }

        public uint PingsFailedByTimeoutCount => throw new Exception("What is 'PingsFailedByTimeoutCount' for round-robin strategy?");

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposing, 1) > 0)
            {
                return;
            }

            foreach (var node in _droppableNodeLogicalConnections.Keys)
            {
                var prevConnection = _droppableNodeLogicalConnections.ContainsKey(node) ? _droppableNodeLogicalConnections[node] : null;
                _droppableNodeLogicalConnections[node] = null;
                prevConnection?.Dispose();
            }
            //Interlocked.Exchange(ref _timer, null)?.Dispose();
        }

        public async Task Connect()
        {
            var connectToAllNodesTasks = _clientOptions.ConnectionOptions.Nodes.Select(n => Connect(n));
            await Task.WhenAll(connectToAllNodesTasks);
        }

        public bool IsConnected() => throw new Exception("What is 'connected' for round-robin strategy?");

        private async Task<LogicalConnection> Connect(TarantoolNode node)
        {
            if (IsConnectedInternal(node))
            {
                return GetConnection(node);
            }

            var semaphore = _nodeConnectionSemaphores[node];
            if (!semaphore.Wait(_connectionTimeout))
            {
                throw ExceptionHelper.NotConnected();
            }

            try
            {
                if (IsConnectedInternal(node))
                {
                    return GetConnection(node);
                }

                _clientOptions.LogWriter?.WriteLine($"{nameof(RoundRobinLogicalConnectionManager)}: Connecting to {node.Uri}...");

                var newConnection = new LogicalConnection(_clientOptions, node, _requestIdCounter);
                await newConnection.Connect().ConfigureAwait(false);

                var prevConnection = _droppableNodeLogicalConnections.ContainsKey(node) ? _droppableNodeLogicalConnections[node] : null;
                _droppableNodeLogicalConnections[node] = newConnection;
                prevConnection?.Dispose();

                _clientOptions.LogWriter?.WriteLine($"{nameof(RoundRobinLogicalConnectionManager)}: Connected to {node.Uri}...");

                //if (_pingCheckInterval > 0 && _timer == null)
                //{
                //    _timer = new Timer(x => CheckPing(), null, _pingTimerInterval, Timeout.Infinite);
                //}

                return newConnection;
            }
            finally
            {
                semaphore.Release();
            }
        }

        private bool IsConnectedInternal(TarantoolNode node)
        {
            return GetConnection(node)?.IsConnected() ?? false;
        }

        private LogicalConnection GetConnection(TarantoolNode node)
        {
            return _droppableNodeLogicalConnections.ContainsKey(node) ?
                _droppableNodeLogicalConnections[node] :
                null;
        }

        private TarantoolNode GetRandomNode()
        {
            var count = _clientOptions.ConnectionOptions.Nodes.Count;
            return _clientOptions.ConnectionOptions.Nodes[new Random().Next(count)];
        }


        //private static readonly PingRequest _pingRequest = new PingRequest();

        //private void CheckPing()
        //{
        //    try
        //    {
        //        if (_nextPingTime > DateTimeOffset.UtcNow)
        //        {
        //            return;
        //        }

        //        SendRequestWithEmptyResponse(_pingRequest, _pingTimeout).GetAwaiter().GetResult();
        //    }
        //    catch (Exception e)
        //    {
        //        _clientOptions.LogWriter?.WriteLine($"{nameof(RoundRobinLogicalConnectionManager)}: Ping failed with exception: {e.Message}. Dropping current connection.");
        //        _droppableLogicalConnection?.Dispose();
        //    }
        //    finally
        //    {
        //        if (_disposing == 0)
        //        {
        //            _timer?.Change(_pingTimerInterval, Timeout.Infinite);
        //        }
        //    }
        //}

        //private bool IsConnectedInternal()
        //{
        //    return _droppableLogicalConnection?.IsConnected() ?? false;
        //}

        //private void ScheduleNextPing()
        //{
        //    if (_pingCheckInterval > 0)
        //    {
        //        _nextPingTime = DateTimeOffset.UtcNow.AddMilliseconds(_pingCheckInterval);
        //    }
        //}

        public async Task<DataResponse<TResponse[]>> SendRequest<TRequest, TResponse>(TRequest request, TimeSpan? timeout = null)
            where TRequest : IRequest
        {
            var node = GetRandomNode();
            var connection = await Connect(node).ConfigureAwait(false);

            var result = await connection.SendRequest<TRequest, TResponse>(request, timeout).ConfigureAwait(false);

            //ScheduleNextPing();

            return result;
        }

        public async Task<DataResponse> SendRequest<TRequest>(TRequest request, TimeSpan? timeout = null) where TRequest : IRequest
        {
            var node = GetRandomNode();
            var connection = await Connect(node).ConfigureAwait(false);

            var result = await connection.SendRequest(request, timeout).ConfigureAwait(false);

            //ScheduleNextPing();

            return result;
        }

        public async Task<byte[]> SendRawRequest<TRequest>(TRequest request, TimeSpan? timeout = null)
            where TRequest : IRequest
        {
            var node = GetRandomNode();
            var connection = await Connect(node).ConfigureAwait(false);

            var result = await connection.SendRawRequest(request, timeout).ConfigureAwait(false);

            //ScheduleNextPing();

            return result;
        }

        public async Task SendRequestWithEmptyResponse<TRequest>(TRequest request, TimeSpan? timeout = null) where TRequest : IRequest
        {
            var node = GetRandomNode();
            var connection = await Connect(node).ConfigureAwait(false);

            await connection.SendRequestWithEmptyResponse(request, timeout).ConfigureAwait(false);

            //ScheduleNextPing();
        }
    }
}
