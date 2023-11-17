using System.Collections.Generic;
using System.Threading;

namespace ProGaudi.Tarantool.Client.Pool
{
    internal class RoundRobinPool<TKey, TObj> : IPoolStrategy<TKey, TObj>
        where TObj : class
    {
        private readonly ReaderWriterLockSlim _lock;
        private readonly List<TObj> _connections;
        private readonly Dictionary<TKey, int> _indexByAddr;
        private int _current;

        public RoundRobinPool()
        {
            _lock = new ReaderWriterLockSlim();
            _connections = new List<TObj>();
            _indexByAddr = new Dictionary<TKey, int>();
            _current = 0;
        }

        public TObj GetNext()
        {
            _lock.EnterReadLock();
            try
            {
                if (_connections.Count == 0)
                {
                    return null;
                }
                
                var updatedCurrentIndex = Interlocked.Increment(ref _current);
                var nextIndex = (updatedCurrentIndex - 1) % _connections.Count;

                return _connections[nextIndex];
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }

        public TObj GetByKey(TKey key)
        {
            _lock.EnterReadLock();
            try
            {
                return !_indexByAddr.ContainsKey(key) ? 
                    null : 
                    _connections[_indexByAddr[key]];
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }

        public TObj[] GetAll()
        {
            _lock.EnterReadLock();
            try
            {
                var result = new TObj[_connections.Count];
                _connections.CopyTo(result, 0);
                return result;
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }

        public void Add(TKey key, TObj obj)
        {
            _lock.EnterWriteLock();
            try
            {
                if (_indexByAddr.TryGetValue(key, out var existedIndex))
                {
                    _connections[existedIndex] = obj;
                }
                else
                {
                    _connections.Add(obj);
                    _indexByAddr[key] = _connections.Count - 1;
                }
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }

        public TObj DeleteByKey(TKey key)
        {
            _lock.EnterWriteLock();
            try
            {
                if (_connections.Count == 0)
                {
                    return null;
                }

                if (!_indexByAddr.TryGetValue(key, out var index))
                {
                    return null;
                }

                _indexByAddr.Remove(key);

                var conn = _connections[index];
                _connections.RemoveAt(index);

                foreach (var k in _indexByAddr.Keys)
                {
                    if (_indexByAddr[k] > index)
                    {
                        _indexByAddr[k] -= 1;
                    }
                }

                return conn;
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }

        public bool IsEmpty()
        {
            return _connections.Count == 0;
        }

        ~RoundRobinPool()
        {
            _lock?.Dispose();
        }
    }
}