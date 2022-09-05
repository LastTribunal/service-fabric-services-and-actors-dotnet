using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace KVSToRCPerformanceTesterService
{
    internal static class HttpClientUtility
    {
        public static HttpClient httpClient;

        static HttpClientUtility()
        {
            httpClient = new HttpClient();
            httpClient.Timeout = TimeSpan.FromMinutes(2);
        }

        public static async Task<string> GetAsync(string url)
        {
            var response = await httpClient.GetAsync(url);
            return await response.Content.ReadAsStringAsync();
        }

        public static async Task<string> PostAsync(string url)
        {
            var response = await httpClient.PostAsync(url, null);
            return response.Content.ToString();
        }

        public static async Task<string> PostAsync(string url, HttpContent content)
        {
            var response = await httpClient.PostAsync(url, content);
            return response.Content.ToString();
        }
    }
}
