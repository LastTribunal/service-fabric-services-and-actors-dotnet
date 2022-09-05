using Microsoft.ApplicationInsights.Extensibility;
using Serilog;
using Serilog.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using static KVSToRCPerformanceTesterService.Constants;

namespace KVSToRCPerformanceTesterService
{
    internal class Utils
    {
        static Logger log;

        static Utils()
        {
            log = new LoggerConfiguration()
                    .WriteTo.ApplicationInsights(new TelemetryConfiguration { InstrumentationKey = ApplicationInsightsInstrumentationKey }, TelemetryConverter.Traces)
                    .CreateLogger();
        }

        internal static int ConvertToInt(string str)
        {
            return int.Parse(str);
        }

        internal static void Log(string msg)
        {
            log.Information(msg);
        }

        internal static void LogVerbose(string msg)
        {
            log.Verbose(msg);
        }

        internal static void Log(object obj)
        {
            Log(obj.ToString());
        }
    }
}
