using System.Collections.Generic;
using Microsoft.AspNetCore.Mvc;

namespace NyBikesAnalytics.API.Controllers
{
    [Route("api/[controller]")]
    public class ValuesController : ControllerBase
    {

        // GET api/values
        [HttpGet]
        public IEnumerable<string> Get()
        {
            return new string[] { "value1", "value2" };
        }
    }
}
