using System;
using System.Collections.Generic;
using System.Text;

namespace FuncBlobs.LoadGenerator
{
    public class LoadJob
    {
        public string Account { get; set; }
        public string Container { get; set; }
        public int Size { get; set; }
    }
}
