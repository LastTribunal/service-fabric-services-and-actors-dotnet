using Microsoft.ServiceFabric.Services.Remoting;
using System.Threading.Tasks;

namespace KVSActor.Interfaces
{
    public interface IKVSActorService : IService
    {
        Task PerFormBackupAsync();
    }
}
