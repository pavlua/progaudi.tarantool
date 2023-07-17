using System;
using System.IO;

namespace ProGaudi.Tarantool.Client
{
    public class TextWriterLog : ILog
    {
        protected readonly TextWriter InternalWriter;

        public TextWriterLog(TextWriter textWriter)
        {
            InternalWriter = textWriter;
        }

        public void WriteLine(string message)
        {
            InternalWriter.WriteLine($"[{DateTime.Now:yyyy-MM-ddTHH:mm:ss.fff}]" + message);
        }

        public void Flush()
        {
            InternalWriter.Flush();
        }
    }
}