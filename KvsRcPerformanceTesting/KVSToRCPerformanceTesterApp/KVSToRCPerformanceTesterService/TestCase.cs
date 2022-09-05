using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Json;
using System.Text;
using System.Threading.Tasks;

namespace KVSToRCPerformanceTesterService
{
    [DataContract]
    internal class TestCase
    {
        private static DataContractJsonSerializer serializer = new DataContractJsonSerializer(typeof(TestCase), new DataContractJsonSerializerSettings
        {
            UseSimpleDictionaryFormat = true,
        });

        [DataMember]
        public string TestId { get; set; }

        [DataMember]
        public int KeyValuePairsPerChunk { get; set; }

        [DataMember]
        public int CopyPhaseParallelism { get; set; }

        [DataMember]
        public int ChunksPerEnumeration { get; set; }

        [DataMember]
        public int DowntimeThreshold { get; set; }

        [DataMember]
        public int Index { get; set; }

        public override string ToString()
        {
            using (var stream = new MemoryStream())
            {
                serializer.WriteObject(stream, this);

                var returnVal = Encoding.ASCII.GetString(stream.GetBuffer());

                return returnVal;
            }
        }
    }
}
