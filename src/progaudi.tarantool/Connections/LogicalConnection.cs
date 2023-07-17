using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ProGaudi.MsgPack.Light;
using ProGaudi.Tarantool.Client.Model;
using ProGaudi.Tarantool.Client.Model.Headers;
using ProGaudi.Tarantool.Client.Model.Requests;
using ProGaudi.Tarantool.Client.Model.Responses;
using ProGaudi.Tarantool.Client.Utils;

namespace ProGaudi.Tarantool.Client.Connections
{
    internal class LogicalConnection : ILogicalConnection
    {
        private readonly MsgPackContext _msgPackContext;

        private readonly RequestIdCounter _requestIdCounter;

        private readonly IPhysicalConnection _physicalConnection;

        private readonly IResponseReader _responseReader;

        private readonly IRequestWriter _requestWriter;

        private readonly ILog _logWriter;

        private readonly TarantoolNode _node;

        private bool _disposed;

        private readonly ConnectionPinger _connectionPinger;

        public LogicalConnection(ClientOptions options, TarantoolNode node, RequestIdCounter requestIdCounter)
        {
            _requestIdCounter = requestIdCounter;
            _msgPackContext = options.MsgPackContext;
            _logWriter = options.LogWriter;

            _physicalConnection = new NetworkStreamPhysicalConnection(options, node);
            _responseReader = new ResponseReader(options, _physicalConnection);
            _requestWriter = new RequestWriter(options, _physicalConnection);
            _connectionPinger = new ConnectionPinger(options, this, node);
            _node = node;
        }

        public uint PingsFailedByTimeoutCount { get; private set; }

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;

            _connectionPinger.Dispose();
            _responseReader.Dispose();
            _requestWriter.Dispose();
            _physicalConnection.Dispose();
        }

        public async Task Connect()
        {
            await _physicalConnection.Connect().ConfigureAwait(false);

            var greetingsResponseBytes = new byte[128];
            var readCount = await _physicalConnection.ReadAsync(greetingsResponseBytes, 0, greetingsResponseBytes.Length).ConfigureAwait(false);
            if (readCount != greetingsResponseBytes.Length)
            {
                throw ExceptionHelper.UnexpectedGreetingBytesCount(readCount);
            }

            var greetings = new GreetingsResponse(greetingsResponseBytes);

            _logWriter?.WriteLine($"Greetings received, salt is {Convert.ToBase64String(greetings.Salt)} .");

            PingsFailedByTimeoutCount = 0;

            _responseReader.BeginReading();
            _requestWriter.BeginWriting();

            _logWriter?.WriteLine("Server responses reading started.");

            await LoginIfNotGuest(greetings).ConfigureAwait(false);
            _connectionPinger.Setup();
        }

        public bool IsConnected()
        {
            if (_disposed)
            {
                return false;
            }

            return _responseReader.IsConnected && _requestWriter.IsConnected && _physicalConnection.IsConnected;
        }

        public async Task SendRequestWithEmptyResponse<TRequest>(TRequest request, TimeSpan? timeout = null)
            where TRequest : IRequest
        {
            await SendRequestImpl(request, timeout).ConfigureAwait(false);
        }

        public async Task<DataResponse<TResponse[]>> SendRequest<TRequest, TResponse>(TRequest request, TimeSpan? timeout = null)
            where TRequest : IRequest
        {
            var stream = await SendRequestImpl(request, timeout).ConfigureAwait(false);
            return MsgPackSerializer.Deserialize<DataResponse<TResponse[]>>(stream, _msgPackContext);
        }

        public async Task<DataResponse> SendRequest<TRequest>(TRequest request, TimeSpan? timeout = null)
            where TRequest : IRequest
        {
            var stream = await SendRequestImpl(request, timeout).ConfigureAwait(false);
            return MsgPackSerializer.Deserialize<DataResponse>(stream, _msgPackContext);
        }

        public async Task<byte[]> SendRawRequest<TRequest>(TRequest request, TimeSpan? timeout = null)
            where TRequest : IRequest
        {
            return (await SendRequestImpl(request, timeout).ConfigureAwait(false)).ToArray();
        }

        private async Task LoginIfNotGuest(GreetingsResponse greetings)
        {
            if (string.IsNullOrEmpty(_node.Uri.UserName))
            {
                _logWriter?.WriteLine("Guest mode, no authentication attempt.");
                return;
            }

            var authenticateRequest = AuthenticationRequest.Create(greetings, _node.Uri);

            await SendRequestWithEmptyResponse(authenticateRequest).ConfigureAwait(false);
            _logWriter?.WriteLine($"Authentication request send: {authenticateRequest}");
        }

        private async Task<MemoryStream> SendRequestImpl<TRequest>(TRequest request, TimeSpan? timeout)
            where TRequest : IRequest
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(LogicalConnection));
            }

            var requestId = _requestIdCounter.GetRequestId();
            var responseTask = _responseReader.GetResponseTask(requestId);

            var stream = CreateAndSerializeHeader(request, requestId);
            MsgPackSerializer.Serialize(request, stream, _msgPackContext);
            var totalLength = stream.Position - Constants.PacketSizeBufferSize;
            var packetLength = new PacketSize((uint)totalLength);
            AddPacketSize(stream, packetLength);

            ArraySegment<byte> buffer;
            if (!stream.TryGetBuffer(out buffer))
            {
                throw new InvalidOperationException("broken buffer");
            }

            //keep API for the sake of backward comp.
            _requestWriter.Write(
                // merged header and body
                buffer);

            try
            {
                if (timeout.HasValue)
                {
                    var cts = new CancellationTokenSource(timeout.Value);
                    responseTask = responseTask.WithCancellation(cts.Token);
                }

                var responseStream = await responseTask.ConfigureAwait(false);
                _logWriter?.WriteLine($"Response with requestId {requestId} is recieved, length: {responseStream.Length}.");

                _connectionPinger.ScheduleNextPing();

                return responseStream;
            }
            catch (ArgumentException)
            {
                _logWriter?.WriteLine($"Response with requestId {requestId} failed, content:\n{buffer.ToReadableString()} ");
                throw;
            }
            catch (TimeoutException)
            {
                PingsFailedByTimeoutCount++;
                throw;
            }
        }

        private MemoryStream CreateAndSerializeHeader<TRequest>(
            TRequest request,
            RequestId requestId) where TRequest : IRequest
        {
            var stream = new MemoryStream();

            var requestHeader = new RequestHeader(request.Code, requestId);
            stream.Seek(Constants.PacketSizeBufferSize, SeekOrigin.Begin);
            MsgPackSerializer.Serialize(requestHeader, stream, _msgPackContext);

            return stream;
        }

        private void AddPacketSize(MemoryStream stream, PacketSize packetLength)
        {
            stream.Seek(0, SeekOrigin.Begin);
            MsgPackSerializer.Serialize(packetLength, stream, _msgPackContext);
        }
    }
}
