using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using UhooIndexer.Utilities;

namespace UhooIndexer.MgIndex
{
    #region [   KeyStoreString   ]
    internal class KeyStoreString : IDisposable
    {
        public KeyStoreString(string filename, bool caseSensitve)
        {
            _db = KeyStore<int>.Open(filename, true);
            _caseSensitive = caseSensitve;
        }

        readonly bool _caseSensitive;
        KeyStore<int> _db;

        public void Set(string key, string val)
        {
            Set(key, Encoding.Unicode.GetBytes(val));
        }

        public void Set(string key, byte[] val)
        {
            var str = (_caseSensitive ? key : key.ToLower());
            var bkey = Encoding.Unicode.GetBytes(str);
            var hc = (int)Helper.MurMur.Hash(bkey);
            var ms = new MemoryStream();
            ms.Write(Helper.GetBytes(bkey.Length, false), 0, 4);
            ms.Write(bkey, 0, bkey.Length);
            ms.Write(val, 0, val.Length);

            _db.SetBytes(hc, ms.ToArray());
        }

        public bool Get(string key, out string val)
        {
            val = null;
            byte[] bval;
            var b = Get(key, out bval);
            if (b)
            {
                val = Encoding.Unicode.GetString(bval);
            }
            return b;
        }

        public bool Get(string key, out byte[] val)
        {
            var str = (_caseSensitive ? key : key.ToLower());
            val = null;
            var bkey = Encoding.Unicode.GetBytes(str);
            var hc = (int)Helper.MurMur.Hash(bkey);

            if (_db.GetBytes(hc, out val))
            {
                // unpack data
                byte[] g = null;
                if (UnpackData(val, out val, out g))
                {
                    if (Helper.CompareMemCmp(bkey, g) != 0)
                    {
                        // if data not equal check duplicates (hash conflict)
                        var ints = new List<int>(_db.GetDuplicates(hc));
                        ints.Reverse();
                        foreach (var i in ints)
                        {
                            var bb = _db.FetchRecordBytes(i);
                            if (UnpackData(bb, out val, out g))
                            {
                                if (Helper.CompareMemCmp(bkey, g) == 0)
                                    return true;
                            }
                        }
                        return false;
                    }
                    return true;
                }
            }
            return false;
        }

        public int Count()
        {
            return (int)_db.Count();
        }

        public int RecordCount()
        {
            return (int)_db.RecordCount();
        }

        public void SaveIndex()
        {
            _db.SaveIndex();
        }

        public void Shutdown()
        {
            _db.Shutdown();
        }

        public void Dispose()
        {
            _db.Shutdown();
        }

        private static bool UnpackData(byte[] buffer, out byte[] val, out byte[] key)
        {
            var len = Helper.ToInt32(buffer, 0, false);
            key = new byte[len];
            Buffer.BlockCopy(buffer, 4, key, 0, len);
            val = new byte[buffer.Length - 4 - len];
            Buffer.BlockCopy(buffer, 4 + len, val, 0, buffer.Length - 4 - len);

            return true;
        }

        public string ReadData(int recnumber)
        {
            byte[] val;
            byte[] key;
            var b = _db.FetchRecordBytes(recnumber);
            if (UnpackData(b, out val, out key))
            {
                return Encoding.Unicode.GetString(val);
            }
            return "";
        }

        internal void FreeMemory()
        {
            _db.FreeMemory();
        }
    }
    #endregion

    #region [   KeyStoreGuid  removed ]
    //internal class KeyStoreGuid : IDisposable //, IDocStorage
    //{
    //    public KeyStoreGuid(string filename)
    //    {
    //        _db = KeyStore<int>.Open(filename, true);
    //    }

    //    KeyStore<int> _db;

    //    public void Set(Guid key, string val)
    //    {
    //        Set(key, Encoding.Unicode.GetBytes(val));
    //    }

    //    public int Set(Guid key, byte[] val)
    //    {
    //        byte[] bkey = key.ToByteArray();
    //        int hc = (int)Helper.MurMur.Hash(bkey);
    //        MemoryStream ms = new MemoryStream();
    //        ms.Write(Helper.GetBytes(bkey.Length, false), 0, 4);
    //        ms.Write(bkey, 0, bkey.Length);
    //        ms.Write(val, 0, val.Length);

    //        return _db.SetBytes(hc, ms.ToArray());
    //    }

    //    public bool Get(Guid key, out string val)
    //    {
    //        val = null;
    //        byte[] bval;
    //        bool b = Get(key, out bval);
    //        if (b)
    //        {
    //            val = Encoding.Unicode.GetString(bval);
    //        }
    //        return b;
    //    }

    //    public bool Get(Guid key, out byte[] val)
    //    {
    //        val = null;
    //        byte[] bkey = key.ToByteArray();
    //        int hc = (int)Helper.MurMur.Hash(bkey);

    //        if (_db.Get(hc, out val))
    //        {
    //            // unpack data
    //            byte[] g = null;
    //            if (UnpackData(val, out val, out g))
    //            {
    //                if (Helper.CompareMemCmp(bkey, g) != 0)
    //                {
    //                    // if data not equal check duplicates (hash conflict)
    //                    List<int> ints = new List<int>(_db.GetDuplicates(hc));
    //                    ints.Reverse();
    //                    foreach (int i in ints)
    //                    {
    //                        byte[] bb = _db.FetchRecordBytes(i);
    //                        if (UnpackData(bb, out val, out g))
    //                        {
    //                            if (Helper.CompareMemCmp(bkey, g) == 0)
    //                                return true;
    //                        }
    //                    }
    //                    return false;
    //                }
    //                return true;
    //            }
    //        }
    //        return false;
    //    }

    //    public void SaveIndex()
    //    {
    //        _db.SaveIndex();
    //    }

    //    public void Shutdown()
    //    {
    //        _db.Shutdown();
    //    }

    //    public void Dispose()
    //    {
    //        _db.Shutdown();
    //    }

    //    public byte[] FetchRecordBytes(int record)
    //    {
    //        return _db.FetchRecordBytes(record);
    //    }

    //    public int Count()
    //    {
    //        return (int)_db.Count();
    //    }

    //    public int RecordCount()
    //    {
    //        return (int)_db.RecordCount();
    //    }

    //    private bool UnpackData(byte[] buffer, out byte[] val, out byte[] key)
    //    {
    //        int len = Helper.ToInt32(buffer, 0, false);
    //        key = new byte[len];
    //        Buffer.BlockCopy(buffer, 4, key, 0, len);
    //        val = new byte[buffer.Length - 4 - len];
    //        Buffer.BlockCopy(buffer, 4 + len, val, 0, buffer.Length - 4 - len);

    //        return true;
    //    }

    //    internal byte[] Get(int recnumber, out Guid docid)
    //    {
    //        bool isdeleted = false;
    //        return Get(recnumber, out docid, out isdeleted);
    //    }

    //    public bool RemoveKey(Guid key)
    //    {
    //        byte[] bkey = key.ToByteArray();
    //        int hc = (int)Helper.MurMur.Hash(bkey);
    //        MemoryStream ms = new MemoryStream();
    //        ms.Write(Helper.GetBytes(bkey.Length, false), 0, 4);
    //        ms.Write(bkey, 0, bkey.Length);
    //        return _db.Delete(hc, ms.ToArray());
    //    }

    //    public byte[] Get(int recnumber, out Guid docid, out bool isdeleted)
    //    {
    //        docid = Guid.Empty;
    //        byte[] buffer = _db.FetchRecordBytes(recnumber, out isdeleted);
    //        if (buffer == null) return null;
    //        if (buffer.Length == 0) return null;
    //        byte[] key;
    //        byte[] val;
    //        // unpack data
    //        UnpackData(buffer, out val, out key);
    //        docid = new Guid(key);
    //        return val;
    //    }

    //    internal int CopyTo(StorageFile<int> backup, int start)
    //    {
    //        return _db.CopyTo(backup, start);
    //    }
    //}
    #endregion

    internal class KeyStore<T> : IDisposable, IDocStorage<T> where T : IComparable<T>
    {
        public KeyStore(string filename, byte maxKeySize, bool allowDuplicateKeys)
        {
            Initialize(filename, maxKeySize, allowDuplicateKeys);
        }

        public KeyStore(string filename, bool allowDuplicateKeys)
        {
            Initialize(filename, Global.DefaultStringKeySize, allowDuplicateKeys);
        }

        private string _path = "";
        private string _fileName = "";
        private byte _maxKeySize;
        private StorageFile<T> _archive;
        private MgIndex<T> _index;
        private const string DatExtension = ".mgdat";
        private const string IdxExtension = ".mgidx";
        private IGetBytes<T> _t;
        private System.Timers.Timer _savetimer;
        private BoolIndex _deleted;


        public static KeyStore<T> Open(string filename, bool allowDuplicateKeys)
        {
            return new KeyStore<T>(filename, allowDuplicateKeys);
        }

        public static KeyStore<T> Open(string filename, byte maxKeySize, bool allowDuplicateKeys)
        {
            return new KeyStore<T>(filename, maxKeySize, allowDuplicateKeys);
        }

        readonly object _savelock = new object();
        public void SaveIndex()
        {
            if (_index == null)
                return;
            lock (_savelock)
            {
                Logging.WriteLog("Saving to disk.", Logging.LogType.Information, Logging.LogCaller.KeyStore);
                _index.SaveIndex();
                _deleted.SaveIndex();
                Logging.WriteLog("Index saved.", Logging.LogType.Information, Logging.LogCaller.KeyStore);
            }
        }

        public IEnumerable<int> GetDuplicates(T key)
        {
            // get duplicates from index
            return _index.GetDuplicates(key);
        }

        public byte[] FetchRecordBytes(int record)
        {
            return _archive.ReadBytes(record);
        }

        public long Count()
        {
            var c = _archive.Count();
            return c - _deleted.GetBits().CountOnes() * 2;
        }

        public bool Get(T key, out string val)
        {
            byte[] b = null;
            val = "";
            var ret = GetBytes(key, out b);
            if (ret)
            {
                if (b != null)
                    val = Encoding.Unicode.GetString(b);
                else
                    val = "";
            }
            return ret;
        }

        public bool GetObject(T key, out object val)
        {
            int off;
            val = null;
            if (_index.Get(key, out off))
            {
                val = _archive.ReadObject(off);
                return true;
            }
            return false;
        }

        public bool GetBytes(T key, out byte[] val)
        {
            int off;
            val = null;
            // search index
            if (_index.Get(key, out off))
            {
                val = _archive.ReadBytes(off);
                return true;
            }
            return false;
        }

        public int SetString(T key, string data)
        {
            return SetBytes(key, Encoding.Unicode.GetBytes(data));
        }

        public int SetObject(T key, object doc)
        {
            var recno = -1;
            // save to storage
            recno = (int) _archive.WriteObject(key, doc);
            // save to index
            _index.Set(key, recno);

            return recno;
        }

        public int SetBytes(T key, byte[] data)
        {
            var recno = -1;
            // save to storage
            recno = (int)_archive.WriteData(key, data);
            // save to index
            _index.Set(key, recno);

            return recno;
        }

        private readonly object _shutdownlock = new object();
        public void Shutdown()
        {
            lock (_shutdownlock)
            {
                if (_index != null)
                    Logging.WriteLog("Shutting down.", Logging.LogType.Information, Logging.LogCaller.KeyStore);

                else
                    return;
                _savetimer.Enabled = false;
                SaveIndex();
                SaveLastRecord();

                if (_deleted != null)
                    _deleted.Shutdown();
                if (_index != null)
                    _index.Shutdown();
                if (_archive != null)
                    _archive.Shutdown();
                _index = null;
                _archive = null;
                _deleted = null;
            }
        }

        public void Dispose()
        {
            Shutdown();
        }

        #region [            P R I V A T E     M E T H O D S              ]
        private void SaveLastRecord()
        {
            // save the last record number in the index file
            _index.SaveLastRecordNumber(_archive.Count());
        }

        private void Initialize(string filename, byte maxkeysize, bool allowDuplicateKeys)
        {
            _maxKeySize = RDBDataType<T>.GetByteSize(maxkeysize);
            _t = RDBDataType<T>.ByteHandler();

            _path = Path.GetDirectoryName(filename);
            Directory.CreateDirectory(_path);

            _fileName = Path.GetFileNameWithoutExtension(filename);
            var db = _path + Path.DirectorySeparatorChar + _fileName + DatExtension;
            var idx = _path + Path.DirectorySeparatorChar + _fileName + IdxExtension;

            _index = new MgIndex<T>(_path, _fileName + IdxExtension, _maxKeySize, /*Global.PageItemCount,*/ allowDuplicateKeys);

            if (Global.SaveAsBinaryJSON)
                _archive = new StorageFile<T>(db, SF_FORMAT.BSON, false);
            else
                _archive = new StorageFile<T>(db, SF_FORMAT.JSON, false);

            _deleted = new BoolIndex(_path, _fileName , "_deleted.idx");
            Logging.WriteLog("Current Count = " + RecordCount().ToString("#,0") +".", Logging.LogType.Information, Logging.LogCaller.KeyStore);
            CheckIndexState();
            Logging.WriteLog("Starting save timer.", Logging.LogType.Information, Logging.LogCaller.KeyStore);
            _savetimer = new System.Timers.Timer();
            _savetimer.Elapsed += new System.Timers.ElapsedEventHandler(_savetimer_Elapsed);
            _savetimer.Interval = Global.SaveIndexToDiskTimerSeconds * 1000;
            _savetimer.AutoReset = true;
            _savetimer.Start();

        }

        private void CheckIndexState()
        {
            Logging.WriteLog("Checking index state.", Logging.LogType.Information, Logging.LogCaller.KeyStore);
            var last = _index.GetLastIndexedRecordNumber();
            var count = _archive.Count();
            if (last < count)
            {
                Logging.WriteLog("Rebuilding index.", Logging.LogType.Information, Logging.LogCaller.KeyStore);
                Logging.WriteLog("   last index count = " + last + ".", Logging.LogType.Information, Logging.LogCaller.KeyStore);
                Logging.WriteLog("   data items count = " + count + ".", Logging.LogType.Information, Logging.LogCaller.KeyStore);
                // Check last index record and archive record and rebuild the index if needed.
                for (var i = last; i < count; i++)
                {
                    var deleted = false;
                    var key = _archive.GetKey(i, out deleted);
                    if (deleted == false)
                        _index.Set(key, i);
                    else
                        _index.RemoveKey(key);

                    if (i % 100000 == 0)
                    Logging.WriteLog("100,000 items re-indexed.", Logging.LogType.Information, Logging.LogCaller.KeyStore);
                }
                Logging.WriteLog("Rebuild index done.", Logging.LogType.Information, Logging.LogCaller.KeyStore);
            }
        }

        void _savetimer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            SaveIndex();
        }

        #endregion

        public int RecordCount()
        {
            return _archive.Count();
        }

        public int[] GetHistory(T key)
        {
            var a = new List<int>();
            foreach (var i in GetDuplicates(key))
            {
                a.Add(i);
            }
            return a.ToArray();
        }

        internal byte[] FetchRecordBytes(int record, out bool isdeleted)
        {
            StorageItem<T> meta;
            var b = _archive.ReadBytes(record, out meta);
            isdeleted = meta.isDeleted;
            return b;
        }

        internal bool Delete(T id)
        {
            // write a delete record
            var rec = (int)_archive.Delete(id);
            _deleted.Set(true, rec);
            return _index.RemoveKey(id);
        }

        internal bool DeleteReplicated(T id)
        {
            // write a delete record for replicated object
            var rec = (int)_archive.DeleteReplicated(id);
            _deleted.Set(true, rec);
            return _index.RemoveKey(id);
        }

        internal int CopyTo(StorageFile<T> storagefile, long startrecord)
        {
            return _archive.CopyTo(storagefile, startrecord);
        }

        public byte[] GetBytes(int rowid, out StorageItem<T> meta)
        {
            return _archive.ReadBytes(rowid, out meta);
        }

        internal void FreeMemory()
        {
            _index.FreeMemory();
        }

        public object GetObject(int rowid, out StorageItem<T> meta)
        {
            return _archive.ReadObject(rowid, out meta);
        }

        public StorageItem<T> GetMeta(int rowid)
        {
            return _archive.ReadMeta(rowid);
        }

        internal int SetReplicationObject(T key, object doc)
        {
            var recno = -1;
            // save to storage
            recno = (int) _archive.WriteReplicationObject(key, doc);
            // save to index
            _index.Set(key, recno);

            return recno;
        }
    }
}
