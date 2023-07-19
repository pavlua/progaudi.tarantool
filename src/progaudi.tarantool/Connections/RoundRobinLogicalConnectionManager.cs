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

        public async Task<DataResponse<TResponse[]>> SendRequest<TRequest, TResponse>(TRequest request, TimeSpan? timeout = null)
            where TRequest : IRequest
        {
            var node = GetRandomNode();
            var connection = await Connect(node).ConfigureAwait(false);

            return await connection.SendRequest<TRequest, TResponse>(request, timeout).ConfigureAwait(false);
        }

        public async Task<DataResponse> SendRequest<TRequest>(TRequest request, TimeSpan? timeout = null) where TRequest : IRequest
        {
            var node = GetRandomNode();
            var connection = await Connect(node).ConfigureAwait(false);

            return await connection.SendRequest(request, timeout).ConfigureAwait(false);
        }

        public async Task<byte[]> SendRawRequest<TRequest>(TRequest request, TimeSpan? timeout = null)
            where TRequest : IRequest
        {
            var node = GetRandomNode();
            var connection = await Connect(node).ConfigureAwait(false);

            return await connection.SendRawRequest(request, timeout).ConfigureAwait(false);
        }

        public async Task SendRequestWithEmptyResponse<TRequest>(TRequest request, TimeSpan? timeout = null) where TRequest : IRequest
        {
            var node = GetRandomNode();
            var connection = await Connect(node).ConfigureAwait(false);

            await connection.SendRequestWithEmptyResponse(request, timeout).ConfigureAwait(false);
        }
    }
}
