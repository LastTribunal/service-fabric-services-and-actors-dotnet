using Microsoft.ServiceFabric.Actors.Migration;
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
    internal class TestResult
    {
        private static DataContractJsonSerializer serializer = new DataContractJsonSerializer(typeof(TestResult), new DataContractJsonSerializerSettings
        {
            UseSimpleDictionaryFormat = true,
        });

        [DataMember]
        public string TestId { get; set; }

        [DataMember]
        public MigrationResult MigrationResult { get; set; }

        [DataMember]
        public MigrationPerformanceResult MigrationPerformanceResult { get; set; }

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
