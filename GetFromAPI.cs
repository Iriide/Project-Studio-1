using Newtonsoft.Json.Linq;
using System.Collections.Concurrent;

namespace SP1
{
    public class GetFromAPI
    {
        // Fields to store the ID, URL for API request, and the ConcurrentBag for storing results.
        private int id;
        private string url;
        private ConcurrentBag<List<String>> cb;

        // Constructor to initialize the class with the ID, URL, and ConcurrentBag.
        public GetFromAPI(int id, string url, ConcurrentBag<List<String>> cb)
        {
            this.id = id;
            this.url = url;
            this.cb = cb;
        }

        // Asynchronous method to fetch weather data from the API.
        public async void GetWeatherAsync()
        {
            HttpClient client = new HttpClient();
            // Send a GET request to the specified URL.
            var response = await client.GetAsync(url);
            int number_of_denied_requests = 0;

            // If the response status is 'TooManyRequests', retry with exponential backoff.
            while (response.StatusCode == System.Net.HttpStatusCode.TooManyRequests)
            {
                Console.WriteLine("Too many requests");
                // Delay the next request to avoid hitting the rate limit.
                System.Threading.Thread.Sleep(Math.Min(2 ^ number_of_denied_requests, 500));
                response = await client.GetAsync(url);
            }

            // Read the response content as a string.
            var responseString = await response.Content.ReadAsStringAsync();
            // Parse the response string as JSON.
            JObject json = JObject.Parse(responseString);

            try
            {
                // Loop through the months (assuming 12 months in a year).
                for (int i = 0; i < 12; i++)
                {
                    // Log and add the data for each month to the ConcurrentBag.
                    System.Console.WriteLine(id.ToString() + " " + i + " " + json["outputs"]["monthly"][i]["H(h)_m"].ToString() + " " + json["outputs"]["monthly"][i]["T2m"].ToString());
                    cb.Add(new List<String> { id.ToString(), i.ToString(), json["outputs"]["monthly"][i]["H(h)_m"].ToString(), json["outputs"]["monthly"][i]["T2m"].ToString() });
                }
            }
            catch(System.NullReferenceException)
            {
                // Log if there is a NullReferenceException (e.g., missing data in the JSON response).
                System.Console.WriteLine(id.ToString() + " System.NullReferenceException");
            }
        }
    }
}
