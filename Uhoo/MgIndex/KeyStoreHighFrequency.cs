using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using UhooIndexer.Utilities;

namespace UhooIndexer.MgIndex
{
    /// <summary>
    /// A high frequency key value store.
    /// </summary>
    internal class KeyStoreHighFrequency : IKeyStoreHighFrequency
    {
        internal class AllocationBlock
        {
            public string Key;
            public byte KeyLength;
            public int DataLength;
            public bool IsCompressed;
            public bool IsBinaryJson;
            public bool DeleteKey;
            public List<int> Blocks = new List<int>();
            public int Blocknumber;
        }

        MgIndex<string> _keys;
        StorageFileHighFrequency _datastore;
        readonly object _lock = new object();
        ushort _blockSize = 2048;
        private const int Kilobyte = 1024;

        readonly byte[] _blockheader = new byte[]{
            0,0,0,0,    // 0  block # (used for validate block reads and rebuild)
            0,0,0,0,    // 4  next block # 
            0,          // 8  flags bits 0:iscompressed  1:isbinary  2:deletekey
            0,0,0,0,    // 9  data length (compute alloc blocks needed)
            0,          // 13 key length 
            0,          // 14 key type 0=guid 1=string
        };
        private readonly string _path = "";
        private string _S = Path.DirectorySeparatorChar.ToString();
        private bool _isDirty;
        private const string DirtyFilename = "temp.$";

        public KeyStoreHighFrequency(string folder)
        {
            _path = folder;
            Directory.CreateDirectory(_path);
            if (_path.EndsWith(_S) == false) _path += _S;

            if (File.Exists(_path + DirtyFilename))
            {
                Logging.WriteLog("Last shutdown failed, rebuilding data files...", Logging.LogType.Error, Logging.LogCaller.KeyStoreHf);
                RebuildDataFiles();
            }
            _datastore = new StorageFileHighFrequency(_path + "data.mghf", Global.HighFrequencyKVDiskBlockSize);
            _keys = new MgIndex<string>(_path, "keys.idx", 255, /*Global.PageItemCount,*/ false);
            _datastore.Initialize();
            _blockSize = _datastore.GetBlockSize();
        }

        // mgindex special storage for strings ctor -> no idx file
        //    use SaveData() GetData()
        public KeyStoreHighFrequency(string folder, string filename)
        {
            _path = folder;
            Directory.CreateDirectory(_path);
            if (_path.EndsWith(_S) == false) _path += _S;

            _datastore = new StorageFileHighFrequency(_path + filename, Global.HighFrequencyKVDiskBlockSize);
            _datastore.Initialize();
            _blockSize = _datastore.GetBlockSize();
        }

        public int CountHf()
        {
            return _keys.Count();
        }

        public object GetObjectHf(string key)
        {
            lock (_lock)
            {
                int alloc;
                if (_keys.Get(key, out alloc))
                {
                    AllocationBlock ab = FillAllocationBlock(alloc);
                    if (ab.DeleteKey == false)
                    {
                        byte[] data = Readblockdata(ab);

                        return FastBinaryJson.BinaryJson.ToObject(data);
                    }
                }
            }

            return null;
        }

        public bool SetObjectHf(string key, object obj)
        {
            byte[] k = Helper.GetBytes(key);
            if (k.Length > 255)
            {
                Logging.WriteLog("Key length > 255 : " + key, Logging.LogType.Error, Logging.LogCaller.KeyStoreHf);
                throw new Exception("Key must be less than 255 characters");
                //return false;
            }
            lock (_lock)
            {
                if (_isDirty == false)
                    WriteDirtyFile();

                AllocationBlock ab = null;
                int firstblock = 0;
                if (_keys.Get(key, out firstblock))// key exists already
                    ab = FillAllocationBlock(firstblock);

                SaveNew(key, k, obj);
                if (ab != null)
                {
                    // free old blocks
                    ab.Blocks.Add(ab.Blocknumber);
                    _datastore.FreeBlocks(ab.Blocks);
                }
                return true;
            }
        }

        public bool DeleteKeyHf(string key)
        {
            lock (_lock)
            {
                int alloc;
                if (_keys.Get(key, out alloc))
                {
                    if (_isDirty == false)
                        WriteDirtyFile();

                    byte[] keybytes = Helper.GetBytes(key);
                    AllocationBlock ab = FillAllocationBlock(alloc);

                    ab.KeyLength = (byte)keybytes.Length;

                    _keys.RemoveKey(key);// remove key from index

                    // write ab
                    ab.DeleteKey = true;
                    ab.DataLength = 0;

                    byte[] header = CreateAllocHeader(ab, keybytes);

                    _datastore.SeekBlock(ab.Blocknumber);
                    _datastore.WriteBlockBytes(header, 0, header.Length);

                    // free old data blocks
                    _datastore.FreeBlocks(ab.Blocks);

                    return true;
                }
            }
            return false;
        }

        public void CompactStorageHf()
        {
            lock (_lock)
            {
                try
                {
                    Logging.WriteLog("Compacting storage file ...", Logging.LogType.Information, Logging.LogCaller.KeyStoreHf);

                    if (Directory.Exists(_path + "temp"))
                        Directory.Delete(_path + "temp", true);

                    KeyStoreHighFrequency newfile = new KeyStoreHighFrequency(_path + "temp");
                    string[] keys = _keys.GetKeys().Cast<string>().ToArray();
                    Logging.WriteLog("Number of keys : " + keys.Length, Logging.LogType.Information, Logging.LogCaller.KeyStoreHf);
                    foreach (var k in keys)
                    {
                        newfile.SetObjectHf(k, GetObjectHf(k));
                    }
                    newfile.Shutdown();
                    Logging.WriteLog("Compact done.", Logging.LogType.Information, Logging.LogCaller.KeyStoreHf);
                    // shutdown and move files and restart here
                    if (Directory.Exists(_path + "old"))
                        Directory.Delete(_path + "old", true);
                    Directory.CreateDirectory(_path + "old");
                    _datastore.Shutdown();
                    _keys.Shutdown();
                    Logging.WriteLog("Moving files...", Logging.LogType.Information, Logging.LogCaller.KeyStoreHf);
                    foreach (var f in Directory.GetFiles(_path, "*.*"))
                        File.Move(f, _path + "old" + _S + Path.GetFileName(f));

                    foreach (var f in Directory.GetFiles(_path + "temp", "*.*"))
                        File.Move(f, _path + Path.GetFileName(f));

                    Directory.Delete(_path + "temp", true);
                    //Directory.Delete(_Path + "old", true); // FEATURE : delete or keep?
                    Logging.WriteLog("Re-opening storage file", Logging.LogType.Information, Logging.LogCaller.KeyStoreHf);
                    _datastore = new StorageFileHighFrequency(_path + "data.mghf", Global.HighFrequencyKVDiskBlockSize);
                    _keys = new MgIndex<string>(_path, "keys.idx", 255, /*Global.PageItemCount,*/ false);

                    _blockSize = _datastore.GetBlockSize();
                }
                catch (Exception ex)
                {
                    Logging.WriteLog(ex.Message, Logging.LogType.Error, Logging.LogCaller.KeyStoreHf);
                }
            }
        }

        public string[] GetKeysHf()
        {
            lock (_lock)
                return _keys.GetKeys().Cast<string>().ToArray(); // FEATURE : dirty !?
        }

        public bool ContainsHf(string key)
        {
            lock (_lock)
            {
                int i = 0;
                return _keys.Get(key, out i);
            }
        }

        internal void Shutdown()
        {
            _datastore.Shutdown();
            if (_keys != null)
                _keys.Shutdown();

            if (File.Exists(_path + DirtyFilename))
                File.Delete(_path + DirtyFilename);
        }

        internal void FreeMemory()
        {
            _keys.FreeMemory();
        }

        #region [  private methods  ]
        private byte[] Readblockdata(AllocationBlock ab)
        {
            byte[] data = new byte[ab.DataLength];
            long offset = 0;
            int len = ab.DataLength;
            int dbsize = _blockSize - _blockheader.Length - ab.KeyLength;
            ab.Blocks.ForEach(x =>
            {
                byte[] b = _datastore.ReadBlock(x);
                int c = len;
                if (c > dbsize) c = dbsize;
                Buffer.BlockCopy(b, _blockheader.Length + ab.KeyLength, data, (int)offset, c);
                offset += c;
                len -= c;
            });
            if (ab.IsCompressed)
                data = MiniLZO.Decompress(data);
            return data;
        }

        private object _dfile = new object();
        private void WriteDirtyFile()
        {
            lock (_dfile)
            {
                _isDirty = true;
                if (File.Exists(_path + DirtyFilename) == false)
                    File.WriteAllText(_path + DirtyFilename, "dirty");
            }
        }

        private void SaveNew(string key, byte[] keybytes, object obj)
        {
            byte[] data;
            AllocationBlock ab = new AllocationBlock();
            ab.Key = key;
            ab.KeyLength = (byte)keybytes.Length;

            data = FastBinaryJson.BinaryJson.ToBjson(obj);
            ab.IsBinaryJson = true;

            if (data.Length > (int)Global.CompressDocumentOverKiloBytes * Kilobyte)
            {
                ab.IsCompressed = true;
                data = MiniLZO.Compress(data);
            }
            ab.DataLength = data.Length;

            int firstblock = InternalSave(keybytes, data, ab);

            // save keys
            _keys.Set(key, firstblock);
        }

        private int InternalSave(byte[] keybytes, byte[] data, AllocationBlock ab)
        {
            int firstblock = _datastore.GetFreeBlockNumber();
            int blocknum = firstblock;
            byte[] header = CreateAllocHeader(ab, keybytes);
            int dblocksize = _blockSize - header.Length;
            int offset = 0;
            // compute data block count
            int datablockcount = (data.Length / dblocksize) + 1;
            // save data blocks
            int counter = 0;
            int len = data.Length;
            while (datablockcount > 0)
            {
                datablockcount--;
                int next = 0;
                if (datablockcount > 0)
                    next = _datastore.GetFreeBlockNumber();

                Buffer.BlockCopy(Helper.GetBytes(counter, false), 0, header, 0, 4);    // set block number
                Buffer.BlockCopy(Helper.GetBytes(next, false), 0, header, 4, 4); // set next pointer

                _datastore.SeekBlock(blocknum);
                _datastore.WriteBlockBytes(header, 0, header.Length);
                int c = len;
                if (c > dblocksize)
                    c = dblocksize;
                _datastore.WriteBlockBytes(data, offset, c);

                if (next > 0)
                    blocknum = next;
                offset += c;
                len -= c;
                counter++;
            }
            return firstblock;
        }

        private byte[] CreateAllocHeader(AllocationBlock ab, byte[] keybytes)
        {
            byte[] alloc = new byte[_blockheader.Length + keybytes.Length];

            if (ab.IsCompressed)
                alloc[8] = 1;
            if (ab.IsBinaryJson)
                alloc[8] += 2;
            if (ab.DeleteKey)
                alloc[8] += 4;

            Buffer.BlockCopy(Helper.GetBytes(ab.DataLength, false), 0, alloc, 9, 4);
            alloc[13] = ab.KeyLength;
            alloc[14] = 1; // string keys for now
            Buffer.BlockCopy(keybytes, 0, alloc, _blockheader.Length, ab.KeyLength);

            return alloc;
        }

        private AllocationBlock FillAllocationBlock(int blocknumber)
        {
            AllocationBlock ab = new AllocationBlock();

            ab.Blocknumber = blocknumber;
            ab.Blocks.Add(blocknumber);

            byte[] b = _datastore.ReadBlockBytes(blocknumber, _blockheader.Length + 255);

            int blocknumexpected = 0;

            int next = ParseBlockHeader(ab, b, blocknumexpected);

            blocknumexpected++;

            while (next > 0)
            {
                ab.Blocks.Add(next);
                b = _datastore.ReadBlockBytes(next, _blockheader.Length + ab.KeyLength);
                next = ParseBlockHeader(ab, b, blocknumexpected);
                blocknumexpected++;
            }

            return ab;
        }

        private int ParseBlockHeader(AllocationBlock ab, byte[] b, int blocknumberexpected)
        {
            int bnum = Helper.ToInt32(b, 0);
            if (bnum != blocknumberexpected)
            {
                Logging.WriteLog("Block numbers does not match, looking for : " + blocknumberexpected, Logging.LogType.Error, Logging.LogCaller.KeyStoreHf);
                //throw new Exception("Block numbers does not match, looking for : " + blocknumberexpected);
                return -1;
            }
            if (b[14] != 1)
            {
                Logging.WriteLog("Expecting string keys only, got : " + b[14], Logging.LogType.Error, Logging.LogCaller.KeyStoreHf);
                //throw new Exception("Expecting string keys only, got : " + b[11]);
                return -1;
            }

            int next = Helper.ToInt32(b, 4);

            if (ab.KeyLength == 0)
            {
                byte flags = b[8];

                if ((flags & 0x01) > 0)
                    ab.IsCompressed = true;
                if ((flags & 0x02) > 0)
                    ab.IsBinaryJson = true;
                if ((flags & 0x04) > 0)
                    ab.DeleteKey = true;

                ab.DataLength = Helper.ToInt32(b, 9);
                byte keylen = b[13];
                ab.KeyLength = keylen;
                ab.Key = Helper.GetString(b, _blockheader.Length, keylen);
            }
            return next;
        }

        private void RebuildDataFiles()
        {
            MgIndex<string> keys = null;
            try
            {
                // remove old free list
                if (File.Exists(_path + "data.bmp"))
                    File.Delete(_path + "data.bmp");

                _datastore = new StorageFileHighFrequency(_path + "data.mghf", Global.HighFrequencyKVDiskBlockSize);
                _blockSize = _datastore.GetBlockSize();
                if (File.Exists(_path + "keys.idx"))
                {
                    Logging.WriteLog("Removing old keys index.", Logging.LogType.Information, Logging.LogCaller.KeyStoreHf);
                    foreach (var f in Directory.GetFiles(_path, "keys.*"))
                        File.Delete(f);
                }

                keys = new MgIndex<string>(_path, "keys.idx", 255, /*Global.PageItemCount,*/ false);

                BitArray visited = new BitArray();

                int c = _datastore.NumberofBlocks();

                for (int i = 0; i < c; i++) // go through blocks
                {
                    if (visited.Get(i))
                        continue;
                    byte[] b = _datastore.ReadBlockBytes(i, _blockheader.Length + 255);
                    int bnum = Helper.ToInt32(b, 0);
                    if (bnum > 0) // check if a start block
                    {
                        visited.Set(i, true);
                        _datastore.FreeBlock(i); // mark as free
                        continue;
                    }

                    AllocationBlock ab = new AllocationBlock();
                    // start block found
                    int blocknumexpected = 0;

                    int next = ParseBlockHeader(ab, b, blocknumexpected);
                    int last = 0;
                    bool freelast = false;
                    AllocationBlock old = null;

                    if (keys.Get(ab.Key, out last))
                    {
                        old = this.FillAllocationBlock(last);
                        freelast = true;
                    }
                    blocknumexpected++;
                    bool failed = false;
                    if (ab.DeleteKey == false)
                    {
                        while (next > 0) // read the blocks
                        {
                            ab.Blocks.Add(next);
                            b = _datastore.ReadBlockBytes(next, _blockheader.Length + ab.KeyLength);
                            next = ParseBlockHeader(ab, b, blocknumexpected);
                            if (next == -1) // non matching block
                            {
                                failed = true;
                                break;
                            }
                            blocknumexpected++;
                        }
                    }
                    else
                    {
                        failed = true;
                        keys.RemoveKey(ab.Key);
                    }
                    // new data ok
                    if (failed == false)
                    {
                        keys.Set(ab.Key, ab.Blocknumber);// valid block found
                        if (freelast)// free the old blocks
                            _datastore.FreeBlocks(old.Blocks);
                    }

                    visited.Set(i, true);
                }

                // all ok delete temp.$ file
                if (File.Exists(_path + DirtyFilename))
                    File.Delete(_path + DirtyFilename);
            }
            catch (Exception ex)
            {
                Logging.WriteLog(ex.Message, Logging.LogType.Error, Logging.LogCaller.KeyStoreHf);
            }
            finally
            {
                Logging.WriteLog("Shutting down files and index", Logging.LogType.Information, Logging.LogCaller.KeyStoreHf);
                _datastore.Shutdown();
                keys.SaveIndex();
                keys.Shutdown();
            }
        }
        #endregion

        internal void FreeBlocks(List<int> list)
        {
            lock (_lock)
                _datastore.FreeBlocks(list);
        }

        internal int SaveData(string key, byte[] data)
        {
            lock (_lock)
            {
                byte[] kb = Helper.GetBytes(key);
                AllocationBlock ab = new AllocationBlock();
                ab.Key = key;
                ab.KeyLength = (byte)kb.Length;
                ab.IsCompressed = false;
                ab.IsBinaryJson = true;
                ab.DataLength = data.Length;

                return InternalSave(kb, data, ab);
            }
        }

        internal byte[] GetData(int blocknumber, List<int> usedblocks)
        {
            lock (_lock)
            {
                AllocationBlock ab = FillAllocationBlock(blocknumber);
                usedblocks = ab.Blocks;
                byte[] data = Readblockdata(ab);

                return data;
            }
        }

        public int Increment(string key, int amount)
        {
            byte[] k = Helper.GetBytes(key);
            if (k.Length > 255)
            {
                Logging.WriteLog("Key length > 255 : " + key, Logging.LogType.Error, Logging.LogCaller.KeyStoreHf);
                throw new Exception("Key must be less than 255 characters");
                //return false;
            }
            lock (_lock)
            {
                if (_isDirty == false)
                    WriteDirtyFile();

                AllocationBlock ab = null;
                int firstblock = 0;
                if (_keys.Get(key, out firstblock))// key exists already
                    ab = FillAllocationBlock(firstblock);

                object obj = amount;
                if (ab.DeleteKey == false)
                {
                    byte[] data = Readblockdata(ab);

                    obj = FastBinaryJson.BinaryJson.ToObject(data);

                    // add here
                    if (obj is int)
                        obj = ((int)obj) + amount;
                    else if (obj is long)
                        obj = ((long)obj) + amount;
                    else if (obj is decimal)
                        obj = ((decimal)obj) + amount;
                    else
                        return (int)obj;
                }

                SaveNew(key, k, obj);
                if (ab != null)
                {
                    // free old blocks
                    ab.Blocks.Add(ab.Blocknumber);
                    _datastore.FreeBlocks(ab.Blocks);
                }
                return (int)obj;
            }
        }

        public int Decrement(string key, int amount)
        {
            return (int)Increment(key, -amount);
        }

        public decimal Increment(string key, decimal amount)
        {
            byte[] k = Helper.GetBytes(key);
            if (k.Length > 255)
            {
                Logging.WriteLog("Key length > 255 : " + key, Logging.LogType.Error, Logging.LogCaller.KeyStoreHf);
                throw new Exception("Key must be less than 255 characters");
                //return false;
            }
            lock (_lock)
            {
                if (_isDirty == false)
                    WriteDirtyFile();

                AllocationBlock ab = null;
                int firstblock = 0;
                if (_keys.Get(key, out firstblock))// key exists already
                    ab = FillAllocationBlock(firstblock);

                object obj = amount;
                if (ab.DeleteKey == false)
                {
                    byte[] data = Readblockdata(ab);

                    obj = FastBinaryJson.BinaryJson.ToObject(data);

                    // add here
                    if (obj is decimal)
                        obj = ((decimal)obj) + amount;
                    else
                        return (decimal)obj;
                }

                SaveNew(key, k, obj);
                if (ab != null)
                {
                    // free old blocks
                    ab.Blocks.Add(ab.Blocknumber);
                    _datastore.FreeBlocks(ab.Blocks);
                }
                return (decimal)obj;
            }
        }

        public decimal Decrement(string key, decimal amount)
        {
            return Increment(key, -amount);
        }
    }
}
