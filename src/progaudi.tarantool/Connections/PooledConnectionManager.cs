using ProGaudi.Tarantool.Client.Model;
using ProGaudi.Tarantool.Client.Model.Requests;
using ProGaudi.Tarantool.Client.Model.Responses;
using ProGaudi.Tarantool.Client.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace ProGaudi.Tarantool.Client.Connections
{
    internal class PooledConnectionManager : ILogicalConnection
    {
        private readonly ClientOptions _clientOptions;

        private readonly RequestIdCounter _requestIdCounter = new RequestIdCounter();

        private readonly TarantoolNodePool _nodePool;

        private readonly Dictionary<TarantoolNode, SemaphoreSlim> _nodeConnectionSemaphores = new Dictionary<TarantoolNode, SemaphoreSlim>();

        private readonly Dictionary<TarantoolNode, SemaphoreSlim> _nodeReconnectionSemaphores = new Dictionary<TarantoolNode, SemaphoreSlim>();

        private readonly Dictionary<TarantoolNode, Timer> _reconnectionTimers = new Dictionary<TarantoolNode, Timer>();

        private readonly Dictionary<TarantoolNode, LogicalConnection> _droppableNodeLogicalConnections = new Dictionary<TarantoolNode, LogicalConnection>();

        private int _disposing;

        private const int _connectionTimeout = 1000;

        private int _baseReconnectionDelay = 5000;

        private int _reconnectAttemptsLimit = 10;

        public PooledConnectionManager(ClientOptions options)
        {
            _clientOptions = options;
            _nodePool = new TarantoolNodePool(_clientOptions.ConnectionOptions.Nodes);
            _nodeConnectionSemaphores = _clientOptions.ConnectionOptions.Nodes.ToDictionary(n => n, _ => new SemaphoreSlim(1, 1));
            _nodeReconnectionSemaphores = _clientOptions.ConnectionOptions.Nodes.ToDictionary(n => n, _ => new SemaphoreSlim(1, 1));
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

            foreach (var node in _reconnectionTimers.Keys)
            {
                var timer = _reconnectionTimers[node];
                _reconnectionTimers[node] = null;
                timer?.Dispose();
            }
        }

        public async Task Connect()
        {
            var connectToAllNodesTasks = _clientOptions.ConnectionOptions.Nodes.Select(n => Connect(n));
            await Task.WhenAll(connectToAllNodesTasks);
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

                _clientOptions.LogWriter?.WriteLine($"{nameof(PooledConnectionManager)}: Connecting to {node.Uri}...");

                LogicalConnection newConnection = null;
                try
                {
                    newConnection = new LogicalConnection(_clientOptions, node, _requestIdCounter);
                    await newConnection.Connect().ConfigureAwait(false);
                }
                catch(Exception ex)
                {
                    _clientOptions.LogWriter?.WriteLine($"{nameof(PooledConnectionManager)}: Error during connection to {node.Uri}. Exception: {ex.Message}");
                    _nodePool.ExtractFromPool(node);
                    // need to start node recovery process here
                    newConnection?.Dispose();
                    throw;
                }

                //newConnection.OnDisposed += StartNodeConnectionRecovery(node);

                var prevConnection = _droppableNodeLogicalConnections.ContainsKey(node) ? _droppableNodeLogicalConnections[node] : null;
                _droppableNodeLogicalConnections[node] = newConnection;
                prevConnection?.Dispose();

                _clientOptions.LogWriter?.WriteLine($"{nameof(PooledConnectionManager)}: Connected to {node.Uri}...");

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
            return _nodePool.GetRandomNode();
        }

        private void SetupRecoveryNode(TarantoolNode node)
        {
            if (_reconnectionTimers.ContainsKey(node)) 
            {
                return;
            }

            var semaphore = _nodeReconnectionSemaphores[node];
            semaphore.Wait();

            if (_reconnectionTimers.ContainsKey(node))
            {
                return;
            }

            try
            {
                var timer = new Timer(x => TryRecoveryNode(x as ConnectionRecoveryState),
                    new ConnectionRecoveryState { Node = node },
                    GetExponentionalReconnectionDelay(1), Timeout.Infinite);
                _reconnectionTimers[node] = timer;
            }
            finally
            {
                semaphore.Release();
            }
        }

        private int GetExponentionalReconnectionDelay(int attempts)
        {
            var exp = 1 << attempts;
            return exp * _baseReconnectionDelay;
        }

        private void TryRecoveryNode(ConnectionRecoveryState state)
        {

            _clientOptions.LogWriter?.WriteLine($"{nameof(PooledConnectionManager)}: Try to reconnect {state.Node.Uri}. Attempt {state.AttemptsCount}...");

            LogicalConnection newConnection = null;
            try
            {
                newConnection = new LogicalConnection(_clientOptions, state.Node, _requestIdCounter);
                newConnection.Connect().ConfigureAwait(false).GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                _clientOptions.LogWriter?.WriteLine($"{nameof(PooledConnectionManager)}: Error during reconnection to {state.Node.Uri}. Exception: {ex.Message}");
                newConnection?.Dispose();
                state.IncrementAttempts();
                
                if (state.AttemptsCount > _reconnectAttemptsLimit)
                {
                    _clientOptions.LogWriter?.WriteLine($"{nameof(PooledConnectionManager)}: Reconnection attemtps to {state.Node.Uri} expired");
                    _reconnectionTimers[state.Node]?.Dispose();
                }
                else
                {
                    if (_disposing == 0)
                    {
                        _reconnectionTimers[state.Node]?.Change(GetExponentionalReconnectionDelay(state.AttemptsCount), Timeout.Infinite);
                    }
                }
            }

            _reconnectionTimers[state.Node]?.Dispose();
            //newConnection.OnDisposed += StartNodeConnectionRecovery(node);

            var prevConnection = _droppableNodeLogicalConnections.ContainsKey(state.Node) ? _droppableNodeLogicalConnections[state.Node] : null;
            _droppableNodeLogicalConnections[state.Node] = newConnection;
            prevConnection?.Dispose();

            _clientOptions.LogWriter?.WriteLine($"{nameof(PooledConnectionManager)}: Reconnected to {state.Node.Uri}");
        }
    }
}
