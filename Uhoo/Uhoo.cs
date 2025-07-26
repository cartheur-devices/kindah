using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using UhooIndexer.FastJson;
using UhooIndexer.MgIndex;
using UhooIndexer.Utilities;

namespace UhooIndexer
{
    public class Uhoo
    {
        private SafeDictionary<string, int> _words = new SafeDictionary<string, int>();
        //private SafeSortedList<string, int> _words = new SafeSortedList<string, int>();
        private BitmapIndex _bitmaps;
        private BoolIndex _deleted;
        private int _lastDocNum;
        private readonly string _fileName = "words";
        private readonly string _path = "";
        private readonly KeyStoreString _docs;
        private readonly bool _docMode;
        private bool _wordschanged;
        private readonly object _lock = new object();
        public string[] Words
        {
            get { CheckLoaded(); return _words.Keys(); }
        }
        public int WordCount
        {
            get { CheckLoaded(); return _words.Count; }
        }
        public int DocumentCount
        {
            get { CheckLoaded(); return _lastDocNum - (int)_deleted.GetBits().CountOnes(); }
        }
        public string IndexPath { get { return _path; } }

        /// <summary>
        /// Initializes a new instance of the <see cref="Uhoo"/> class.
        /// </summary>
        /// <param name="indexPath">The index path.</param>
        /// <param name="fileName">Name of the file.</param>
        /// <param name="docMode">if set to <c>true</c> [document mode].</param>
        public Uhoo(string indexPath, string fileName, bool docMode)
        {
            _path = indexPath;
            _fileName = fileName;
            _docMode = docMode;
            if (_path.EndsWith(Path.DirectorySeparatorChar.ToString(CultureInfo.InvariantCulture)) == false) _path += Path.DirectorySeparatorChar;
            Directory.CreateDirectory(indexPath);

            Logging.WriteLog("Starting Uhoo.", Logging.LogType.Information, Logging.LogCaller.Uhoo);
            Logging.WriteLog("Storage folder = " + _path + ".", Logging.LogType.Information, Logging.LogCaller.Uhoo);

            if (docMode)
            {
                _docs = new KeyStoreString(_path + "files.docs", false);
                // read deleted
                _deleted = new BoolIndex(_path, "_deleted", ".uhoo");
                _lastDocNum = _docs.Count();
            }
            _bitmaps = new BitmapIndex(_path, _fileName + "_uhoo.bmp");
            // read words
            LoadWords();
        }

        private void CheckLoaded()
        {
            if (_wordschanged == false)
            {
                LoadWords();
            }
        }

        private BitArray ExecutionPlan(string filter, int maxsize)
        {
            Logging.WriteLog("Query : " + filter + ".", Logging.LogType.Information, Logging.LogCaller.Uhoo);
            var dt = FastDateTime.Now;
            // query indexes
            var words = filter.Split(' ');
            //bool defaulttoand = true;
            //if (filter.IndexOfAny(new char[] { '+', '-' }, 0) > 0)
            //    defaulttoand = false;

            BitArray found = null; // WAHBitArray.Fill(maxsize);            

            foreach (var s in words)
            {
                int c;
                var not = false;
                var word = s;
                if (s == "") continue;

                var op = Operation.And;
                //if (defaulttoand)
                //    op = OPERATION.AND;

                if (word.StartsWith("+"))
                {
                    op = Operation.Or;
                    word = s.Replace("+", "");
                }

                if (word.StartsWith("-"))
                {
                    op = Operation.Andnot;
                    word = s.Replace("-", "");
                    not = true;
                    if (found == null) // leading with - -> "-oak hill"
                    {
                        found = BitArray.Fill(maxsize);
                    }
                }

                if (word.Contains("*") || word.Contains("?"))
                {
                    var wildbits = new BitArray();

                    // do wildcard search
                    var reg = new Regex("^" + word.Replace("*", ".*").Replace("?", ".") + "$", RegexOptions.IgnoreCase);
                    foreach (var key in _words.Keys())
                    {
                        if (reg.IsMatch(key))
                        {
                            _words.TryGetValue(key, out c);
                            var ba = _bitmaps.GetBitmap(c);

                            wildbits = DoBitOperation(wildbits, ba, Operation.Or, maxsize);
                        }
                    }
                    if (found == null)
                        found = wildbits;
                    else
                    {
                        if (not) // "-oak -*l"
                            found = found.AndNot(wildbits);
                        else if (op == Operation.And)
                            found = found.And(wildbits);
                        else
                            found = found.Or(wildbits);
                    }
                }
                else if (_words.TryGetValue(word.ToLowerInvariant(), out c))
                {
                    // bits logic
                    var ba = _bitmaps.GetBitmap(c);
                    found = DoBitOperation(found, ba, op, maxsize);
                }
                else if (op == Operation.And)
                    found = new BitArray();
            }
            if (found == null)
                return new BitArray();

            // remove deleted docs
            BitArray ret;
            if (_docMode)
                ret = found.AndNot(_deleted.GetBits());
            else
                ret = found;

            Logging.WriteLog("Query time (ms) = " + FastDateTime.Now.Subtract(dt).TotalMilliseconds + ".", Logging.LogType.Information, Logging.LogCaller.Uhoo);
            return ret;
        }
        private static BitArray DoBitOperation(BitArray bits, BitArray c, Operation op, int maxsize)
        {
            if (bits != null)
            {
                switch (op)
                {
                    case Operation.And:
                        bits = bits.And(c);
                        break;
                    case Operation.Or:
                        bits = bits.Or(c);
                        break;
                    case Operation.Andnot:
                        bits = bits.And(c.Not(maxsize));
                        break;
                }
            }
            else
                bits = c;
            return bits;
        }
        private void InternalSave()
        {
            Logging.WriteLog("Saving index.", Logging.LogType.Information, Logging.LogCaller.Uhoo);
            var dt = FastDateTime.Now;
            // save deleted
            if (_deleted != null)
                _deleted.SaveIndex();

            // save docs 
            if (_docMode)
                _docs.SaveIndex();

            if (_bitmaps != null)
                _bitmaps.Commit(false);

            if (_words != null && _wordschanged)
            {
                var ms = new MemoryStream();
                var bw = new BinaryWriter(ms, Encoding.UTF8);

                // save words and bitmaps
                using (var words = new FileStream(_path + _fileName + ".words", FileMode.Create))
                {
                    var keys = _words.Keys();
                    var c = keys.Length;
                    Logging.WriteLog("Key count = " + c + ".", Logging.LogType.Information, Logging.LogCaller.Uhoo);
                    foreach (var key in _words.Keys())
                    {
                        try//FIX : remove when bug found
                        {
                            bw.Write(key);
                            bw.Write(_words[key]);
                        }
                        catch (Exception ex)
                        {
                            Logging.WriteLog(" on key = " + key, Logging.LogType.Error, Logging.LogCaller.Uhoo);
                            throw ex;
                        }
                    }
                    var b = ms.ToArray();
                    words.Write(b, 0, b.Length);
                    words.Flush();
                    words.Close();
                }
            }
            Logging.WriteLog("Save time (ms) = " + FastDateTime.Now.Subtract(dt).TotalMilliseconds + ".", Logging.LogType.Information, Logging.LogCaller.Uhoo);
        }
        private void LoadWords()
        {
            lock (_lock)
            {
                if (_words == null)
                    _words = //new SafeSortedList<string, int>();
                        new SafeDictionary<string, int>();
                if (File.Exists(_path + _fileName + ".words") == false)
                    return;
                // load words
                var b = File.ReadAllBytes(_path + _fileName + ".words");
                if (b.Length == 0)
                    return;
                var ms = new MemoryStream(b);
                var br = new BinaryReader(ms, Encoding.UTF8);
                var s = br.ReadString();
                while (s != "")
                {
                    var off = br.ReadInt32();
                    _words.Add(s, off);
                    try
                    {
                        s = br.ReadString();
                    }
                    catch { s = ""; }
                }
                Logging.WriteLog("Word Count = " + _words.Count, Logging.LogType.Information, Logging.LogCaller.Uhoo);
                _wordschanged = true;
            }
        }
        private void AddtoIndex(int recnum, string text)
        {
            if (string.IsNullOrEmpty(text))
                return;
            text = text.ToLowerInvariant(); // lowercase index 
            string[] keys;
            if (_docMode)
            {
                Logging.WriteLog("Text size = " + text.Length, Logging.LogType.Information, Logging.LogCaller.Uhoo);
                var wordfreq = Tokenizer.GenerateWordFreq(text);
                Logging.WriteLog("Word count = " + wordfreq.Count, Logging.LogType.Information, Logging.LogCaller.Uhoo);
                var kk = wordfreq.Keys;
                keys = new string[kk.Count];
                kk.CopyTo(keys, 0);
            }
            else
            {
                keys = text.Split(' ');
            }

            foreach (var key in keys)
            {
                if (key == "")
                    continue;

                int bmp;
                if (_words.TryGetValue(key, out bmp))
                {
                    _bitmaps.GetBitmap(bmp).Set(recnum, true);
                }
                else
                {
                    bmp = _bitmaps.GetFreeRecordNumber();
                    _bitmaps.SetDuplicate(bmp, recnum);
                    _words.Add(key, bmp);
                }
            }
            _wordschanged = true;
        }
        internal T Fetch<T>(int docnum)
        {
            var b = _docs.ReadData(docnum);
            return Json.ToObject<T>(b);
        }

        
        /// <summary>
        /// Saves this instance.
        /// </summary>
        public void Save()
        {
            lock (_lock)
                InternalSave();
        }
        /// <summary>
        /// Indexes the specified recordnumber.
        /// </summary>
        /// <param name="recordnumber">The recordnumber.</param>
        /// <param name="text">The text.</param>
        public void Index(int recordnumber, string text)
        {
            CheckLoaded();
            AddtoIndex(recordnumber, text);
        }

        public BitArray Query(string filter, int maxsize)
        {
            CheckLoaded();
            return ExecutionPlan(filter, maxsize);
        }

        public int Index(Document doc, bool deleteold)
        {
            CheckLoaded();
            Logging.WriteLog("indexing doc : " + doc.FileName, Logging.LogType.Information, Logging.LogCaller.Uhoo);
            var dt = FastDateTime.Now;

            if (deleteold && doc.DocNumber > -1)
                _deleted.Set(true, doc.DocNumber);

            if (deleteold || doc.DocNumber == -1)
                doc.DocNumber = _lastDocNum++;

            // save doc to disk
            var dstr = Json.ToJson(doc, new JsonParameters { UseExtensions = false });
            _docs.Set(doc.FileName.ToLower(), Encoding.Unicode.GetBytes(dstr));

            Logging.WriteLog("Writing doc to disk (ms) = " + FastDateTime.Now.Subtract(dt).TotalMilliseconds, Logging.LogType.Information, Logging.LogCaller.Uhoo);

            dt = FastDateTime.Now;
            // index doc
            AddtoIndex(doc.DocNumber, doc.Text);
            Logging.WriteLog("Indexing time (ms) = " + FastDateTime.Now.Subtract(dt).TotalMilliseconds, Logging.LogType.Information, Logging.LogCaller.Uhoo);

            return _lastDocNum;
        }

        public IEnumerable<int> FindRows(string filter)
        {
            CheckLoaded();
            var bits = ExecutionPlan(filter, _docs.RecordCount());
            // enumerate records
            return bits.GetBitIndexes();
        }

        public IEnumerable<T> FindDocuments<T>(string filter)
        {
            CheckLoaded();
            var bits = ExecutionPlan(filter, _docs.RecordCount());
            // enumerate documents
            foreach (var i in bits.GetBitIndexes())
            {
                if (i > _lastDocNum - 1)
                    break;
                var b = _docs.ReadData(i);
                var d = Json.ToObject<T>(b, new JsonParameters { ParametricConstructorOverride = true });

                yield return d;
            }
        }

        public IEnumerable<string> FindDocumentFileNames(string filter)
        {
            CheckLoaded();
            var bits = ExecutionPlan(filter, _docs.RecordCount());
            // enumerate documents
            foreach (var i in bits.GetBitIndexes())
            {
                if (i > _lastDocNum - 1)
                    break;
                var b = _docs.ReadData(i);
                var d = (Dictionary<string, object>)Json.Parse(b);

                yield return d["FileName"].ToString();
            }
        }

        public void RemoveDocument(int number)
        {
            // add number to deleted bitmap
            _deleted.Set(true, number);
        }

        public bool RemoveDocument(string filename)
        {
            // remove doc based on filename
            byte[] b;
            if (_docs.Get(filename.ToLower(), out b))
            {
                var d = Json.ToObject<Document>(Encoding.Unicode.GetString(b));
                RemoveDocument(d.DocNumber);
                return true;
            }
            return false;
        }

        public bool IsIndexed(string filename)
        {
            byte[] b;
            return _docs.Get(filename.ToLower(), out b);
        }

        public void OptimizeIndex()
        {
            lock (_lock)
            {
                InternalSave();
                //_bitmaps.Commit(false);
                _bitmaps.Optimize();
            }
        }
        /// <summary>
        /// Shuts down this instance.
        /// </summary>
        public void Shutdown()
        {
            lock (_lock)
            {
                InternalSave();
                if (_deleted != null)
                {
                    _deleted.SaveIndex();
                    _deleted.Shutdown();
                    _deleted = null;
                }

                if (_bitmaps != null)
                {
                    _bitmaps.Commit(Global.FreeBitmapMemoryOnSave);
                    _bitmaps.Shutdown();
                    _bitmaps = null;
                }

                if (_docMode)
                    _docs.Shutdown();
            }
        }

        public void FreeMemory()
        {
            lock (_lock)
            {
                InternalSave();

                if (_deleted != null)
                    _deleted.FreeMemory();

                if (_bitmaps != null)
                    _bitmaps.FreeMemory();

                if (_docs != null)
                    _docs.FreeMemory();

                //_words = null;// new SafeSortedList<string, int>();
                //_loaded = false;
            }
        }

    }
}
