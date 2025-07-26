using System;
using System.IO;
using System.Xml.Serialization;

namespace UhooIndexer
{
    public class Document
    {
        public Document()
        {
            DocNumber = -1;
        }
        public Document(FileInfo fileinfo, string text)
        {
            FileName = fileinfo.FullName;
            ModifiedDate = fileinfo.LastWriteTime;
            FileSize = fileinfo.Length;
            Text = text;
            DocNumber = -1;
        }
        public int DocNumber { get; set; }
        [XmlIgnore]
        public string Text { get; set; }
        public string FileName { get; set; }
        public DateTime ModifiedDate { get; set; }
        public long FileSize;

        public override string ToString()
        {
            return FileName;
        }
    }
}
