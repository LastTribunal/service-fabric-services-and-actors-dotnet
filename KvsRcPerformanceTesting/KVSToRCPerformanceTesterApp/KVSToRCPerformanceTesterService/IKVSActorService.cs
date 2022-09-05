using Microsoft.ServiceFabric.Actors;
using Microsoft.ServiceFabric.Services.Remoting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KVSActor.Interfaces
{
    public interface IKVSActorService : IService
    {
        public Task PerFormBackupAsync();
    }
}
