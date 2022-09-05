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
    internal class MigrationPerformanceResult
    {
        private static DataContractJsonSerializer serializer = new DataContractJsonSerializer(typeof(MigrationPerformanceResult), new DataContractJsonSerializerSettings
        {
            UseSimpleDictionaryFormat = true,
        });

        [DataMember]
        public DateTime? StartDateTimeUTC { get; set; }

        [DataMember]
        public DateTime? EndDateTimeUTC { get; set; }

        [DataMember]
        public double Duration { get; set; }

        [DataMember]
        public CpuUsageResults[] TotalCpuUsageResults { get; set; }

        [DataMember]
        public RamUsageResults[] TotalRamUsageResults { get; set; }

        [DataMember]
        public PhasePerformanceResult[] PhaseResults { get; set; }

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

    [DataContract]
    internal class PhasePerformanceResult
    {
        private static DataContractJsonSerializer serializer = new DataContractJsonSerializer(typeof(PhasePerformanceResult), new DataContractJsonSerializerSettings
        {
            UseSimpleDictionaryFormat = true,
        });

        [DataMember]
        public DateTime? StartDateTimeUTC { get; set; }

        [DataMember]
        public DateTime? EndDateTimeUTC { get; set; }

        [DataMember]
        public double Duration { get; set; }

        [DataMember]
        public MigrationPhase Phase { get; set; }

        [DataMember]
        public CpuUsageResults[] PerPhaseCpuUsageResults { get; set; }

        [DataMember]
        public RamUsageResults[] PerPhaseRamUsageResults { get; set; }

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

    [DataContract]
    internal class CpuUsageResults
    {
        private static DataContractJsonSerializer serializer = new DataContractJsonSerializer(typeof(CpuUsageResults), new DataContractJsonSerializerSettings
        {
            UseSimpleDictionaryFormat = true,
        });

        [DataMember]
        public string Actor { get; set; }

        [DataMember]
        public double PeakCpuUsage { get; set; }

        [DataMember]
        public double AvgCpuUsage { get; set; }

        [DataMember]
        public double MedianCpuUsage { get; set; }

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

    [DataContract]
    internal class RamUsageResults
    {
        private static DataContractJsonSerializer serializer = new DataContractJsonSerializer(typeof(RamUsageResults), new DataContractJsonSerializerSettings
        {
            UseSimpleDictionaryFormat = true,
        });

        [DataMember]
        public string Actor { get; set; }

        [DataMember]
        public double PeakRamUsage { get; set; }

        [DataMember]
        public double AvgRamUsage { get; set; }

        [DataMember]
        public double MedianRamUsage { get; set; }

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
