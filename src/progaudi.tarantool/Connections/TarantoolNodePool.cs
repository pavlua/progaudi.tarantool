using ProGaudi.Tarantool.Client.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ProGaudi.Tarantool.Client.Connections
{
    internal class TarantoolNodePool
    {
        public TarantoolNodePool(List<TarantoolNode> nodes) 
        { 

        }

        public TarantoolNode GetRandomNode()
        {
            throw new NotImplementedException();
        }

        public void ExtractFromPool(TarantoolNode node)
        {
            throw new NotImplementedException();
        }

        public void ReturnToPool(TarantoolNode node)
        {
            throw new NotImplementedException();
        }
    }
}
