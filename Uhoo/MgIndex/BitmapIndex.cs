using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using UhooIndexer.Utilities;

namespace UhooIndexer.MgIndex
{
    internal class BitmapIndex
    {
        public BitmapIndex(string path, string filename)
        {
            _FileName = Path.GetFileNameWithoutExtension(filename);
            _Path = path;
            if (_Path.EndsWith(Path.DirectorySeparatorChar.ToString()) == false)
                _Path += Path.DirectorySeparatorChar.ToString();

            Initialize();
        }

        class L : IDisposable
        {
            BitmapIndex _sc;
            public L(BitmapIndex sc)
            {
                _sc = sc;
                _sc.CheckInternalOP();
            }
            void IDisposable.Dispose()
            {
                _sc.Done();
            }
        }

        private const string RecExt = ".mgbmr";
        private const string BmpExt = ".mgbmp";
        private string _FileName = "";
        private string _Path = "";
        private FileStream _bitmapFileWriteOrg;
        private BufferedStream _bitmapFileWrite;
        private FileStream _bitmapFileRead;
        private FileStream _recordFileRead;
        private FileStream _recordFileWriteOrg;
        private BufferedStream _recordFileWrite;
        private long _lastBitmapOffset = 0;
        private int _lastRecordNumber = 0;
        //private SafeDictionary<int, WAHBitArray> _cache = new SafeDictionary<int, WAHBitArray>();
        private SafeSortedList<int, BitArray> _cache = new SafeSortedList<int, BitArray>();
        //private SafeDictionary<int, long> _offsetCache = new SafeDictionary<int, long>();
        private bool _stopOperations = false;
        private bool _shutdownDone = false;
        private int _workingCount = 0;
        private bool _isDirty = false;

        #region
        public void Shutdown()
        {
            using (new L(this))
            {
                Logging.WriteLog("Shutdown BitmapIndex", Logging.LogType.Information, Logging.LogCaller.BitMapIndex);
                InternalShutdown();
            }
        }

        public int GetFreeRecordNumber()
        {
            using (new L(this))
            {
                var i = _lastRecordNumber++;

                _cache.Add(i, new BitArray());
                return i;
            }
        }

        public void Commit(bool freeMemory)
        {
            if (_isDirty == false)
                return;
            using (new L(this))
            {

                Logging.WriteLog("Writing " + _FileName + ".", Logging.LogType.Information, Logging.LogCaller.BitMapIndex);
                var keys = _cache.Keys();
                Array.Sort(keys);

                foreach (var k in keys)
                {
                    BitArray bmp = null;
                    if (_cache.TryGetValue(k, out bmp) && bmp.IsDirty)
                    {
                        SaveBitmap(k, bmp);
                        bmp.FreeMemory();
                        bmp.IsDirty = false;
                    }
                }
                Flush();
                if (freeMemory)
                {
                    _cache = //new SafeDictionary<int, WAHBitArray>();
                        new SafeSortedList<int, BitArray>();
                    Logging.WriteLog("Freeing cache.", Logging.LogType.Information, Logging.LogCaller.BitMapIndex);
                }
                _isDirty = false;
            }
        }

        public void SetDuplicate(int bitmaprecno, int record)
        {
            using (new L(this))
            {
                BitArray ba = null;

                ba = InternalGetBitmap(bitmaprecno); //GetBitmap(bitmaprecno);

                ba.Set(record, true);
                _isDirty = true;
            }
        }

        public BitArray GetBitmap(int recno)
        {
            using (new L(this))
            {
                return InternalGetBitmap(recno);
            }
        }

        private readonly object _oplock = new object();
        public void Optimize()
        {
            lock (_oplock)
                lock (_readlock)
                    lock (_writelock)
                    {
                        _stopOperations = true;
                        while (_workingCount > 0) Thread.SpinWait(1);
                        Flush();

                        if (File.Exists(_Path + _FileName + "$" + BmpExt))
                            File.Delete(_Path + _FileName + "$" + BmpExt);

                        if (File.Exists(_Path + _FileName + "$" + RecExt))
                            File.Delete(_Path + _FileName + "$" + RecExt);

                        Stream newrec = new FileStream(_Path + _FileName + "$" + RecExt, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
                        Stream newbmp = new FileStream(_Path + _FileName + "$" + BmpExt, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);

                        long newoffset = 0;
                        var c = (int)(_recordFileRead.Length / 8);
                        for (var i = 0; i < c; i++)
                        {
                            var offset = ReadRecordOffset(i);

                            var b = ReadBmpData(offset);
                            if (b == null)
                            {
                                _stopOperations = false;
                                throw new Exception("bitmap index file is corrupted");
                            }

                            newrec.Write(Helper.GetBytes(newoffset, false), 0, 8);
                            newoffset += b.Length;
                            newbmp.Write(b, 0, b.Length);

                        }
                        newbmp.Flush();
                        newbmp.Close();
                        newrec.Flush();
                        newrec.Close();

                        InternalShutdown();

                        File.Delete(_Path + _FileName + BmpExt);
                        File.Delete(_Path + _FileName + RecExt);
                        File.Move(_Path + _FileName + "$" + BmpExt, _Path + _FileName + BmpExt);
                        File.Move(_Path + _FileName + "$" + RecExt, _Path + _FileName + RecExt);

                        Initialize();
                        _stopOperations = false;
                    }
        }

        internal void FreeMemory()
        {
            try
            {
                var free = new List<int>();
                foreach (var b in _cache)
                {
                    if (b.Value.IsDirty == false)
                        free.Add(b.Key);
                }
                Logging.WriteLog("releasing bmp count = " + free.Count + " out of " + _cache.Count, Logging.LogType.Information, Logging.LogCaller.BitMapIndex);
                foreach (var i in free)
                    _cache.Remove(i);
            }
            catch (Exception ex){
                Logging.WriteLog(ex.Message, Logging.LogType.Error, Logging.LogCaller.BitMapIndex);
            }
        }
        #endregion


        #region [  P R I V A T E  ]
        private byte[] ReadBmpData(long offset)
        {
            _bitmapFileRead.Seek(offset, SeekOrigin.Begin);

            var b = new byte[8];

            _bitmapFileRead.Read(b, 0, 8);
            if (b[0] == (byte)'B' && b[1] == (byte)'M' && b[7] == 0)
            {
                var c = Helper.ToInt32(b, 2) * 4 + 8;
                var data = new byte[c];
                _bitmapFileRead.Seek(offset, SeekOrigin.Begin);
                _bitmapFileRead.Read(data, 0, c);
                return data;
            }
            return null;
        }

        private long ReadRecordOffset(int recnum)
        {
            var b = new byte[8];
            var off = ((long)recnum) * 8;
            _recordFileRead.Seek(off, SeekOrigin.Begin);
            _recordFileRead.Read(b, 0, 8);
            return Helper.ToInt64(b, 0);
        }

        private void Initialize()
        {
            _recordFileRead = new FileStream(_Path + _FileName + RecExt, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
            _recordFileWriteOrg = new FileStream(_Path + _FileName + RecExt, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
            _recordFileWrite = new BufferedStream(_recordFileWriteOrg);

            _bitmapFileRead = new FileStream(_Path + _FileName + BmpExt, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
            _bitmapFileWriteOrg = new FileStream(_Path + _FileName + BmpExt, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
            _bitmapFileWrite = new BufferedStream(_bitmapFileWriteOrg);

            _bitmapFileWrite.Seek(0L, SeekOrigin.End);
            _lastBitmapOffset = _bitmapFileWrite.Length;
            _lastRecordNumber = (int)(_recordFileRead.Length / 8);
            _shutdownDone = false;
        }

        private void InternalShutdown()
        {
            var d1 = false;
            var d2 = false;

            if (_shutdownDone == false)
            {
                Flush();
                if (_recordFileWrite.Length == 0) d1 = true;
                if (_bitmapFileWrite.Length == 0) d2 = true;
                _recordFileRead.Close();
                _bitmapFileRead.Close();
                _bitmapFileWriteOrg.Close();
                _recordFileWriteOrg.Close();
                _recordFileWrite.Close();
                _bitmapFileWrite.Close();
                if (d1)
                    File.Delete(_Path + _FileName + RecExt);
                if (d2)
                    File.Delete(_Path + _FileName + BmpExt);
                _recordFileWrite = null;
                _recordFileRead = null;
                _bitmapFileRead = null;
                _bitmapFileWrite = null;
                _recordFileRead = null;
                _recordFileWrite = null;
                _shutdownDone = true;
            }
        }

        private void Flush()
        {
            if (_shutdownDone)
                return;

            if (_recordFileWrite != null)
                _recordFileWrite.Flush();

            if (_bitmapFileWrite != null)
                _bitmapFileWrite.Flush();

            if (_recordFileRead != null)
                _recordFileRead.Flush();

            if (_bitmapFileRead != null)
                _bitmapFileRead.Flush();

            if (_bitmapFileWriteOrg != null)
                _bitmapFileWriteOrg.Flush();

            if (_recordFileWriteOrg != null)
                _recordFileWriteOrg.Flush();
        }

        private readonly object _readlock = new object();
        private BitArray InternalGetBitmap(int recno)
        {
            lock (_readlock)
            {
                var ba = new BitArray();
                if (recno == -1)
                    return ba;

                if (_cache.TryGetValue(recno, out ba))
                {
                    return ba;
                }
                else
                {
                    long offset = 0;
                    //if (_offsetCache.TryGetValue(recno, out offset) == false)
                    {
                        offset = ReadRecordOffset(recno);
                       // _offsetCache.Add(recno, offset);
                    }
                    ba = LoadBitmap(offset);

                    _cache.Add(recno, ba);

                    return ba;
                }
            }
        }

        private object _writelock = new object();
        private void SaveBitmap(int recno, BitArray bmp)
        {
            lock (_writelock)
            {
                var offset = SaveBitmapToFile(bmp);
                //long v;
                //if (_offsetCache.TryGetValue(recno, out v))
                //    _offsetCache[recno] = offset;
                //else
                //    _offsetCache.Add(recno, offset);

                var pointer = ((long)recno) * 8;
                _recordFileWrite.Seek(pointer, SeekOrigin.Begin);
                var b = new byte[8];
                b = Helper.GetBytes(offset, false);
                _recordFileWrite.Write(b, 0, 8);
            }
        }

        //-----------------------------------------------------------------
        // BITMAP FILE FORMAT
        //    0  'B','M'
        //    2  uint count = 4 bytes
        //    6  Bitmap type :
        //                0 = int record list   
        //                1 = uint bitmap
        //                2 = rec# indexes
        //    7  '0'
        //    8  uint data
        //-----------------------------------------------------------------
        private long SaveBitmapToFile(BitArray bmp)
        {
            var off = _lastBitmapOffset;
            BitArray.Type t;
            var bits = bmp.GetCompressed(out t);

            var b = new byte[bits.Length * 4 + 8];
            // write header data
            b[0] = ((byte)'B');
            b[1] = ((byte)'M');
            Buffer.BlockCopy(Helper.GetBytes(bits.Length, false), 0, b, 2, 4);

            b[6] = (byte)t;
            b[7] = (byte)(0);

            for (var i = 0; i < bits.Length; i++)
            {
                var u = Helper.GetBytes((int)bits[i], false);
                Buffer.BlockCopy(u, 0, b, i * 4 + 8, 4);
            }
            _bitmapFileWrite.Write(b, 0, b.Length);
            _lastBitmapOffset += b.Length;
            return off;
        }

        private BitArray LoadBitmap(long offset)
        {
            var bc = new BitArray();
            if (offset == -1)
                return bc;

            var ar = new List<uint>();
            var type = BitArray.Type.Wah;
            var bmp = _bitmapFileRead;
            {
                bmp.Seek(offset, SeekOrigin.Begin);

                var b = new byte[8];

                bmp.Read(b, 0, 8);
                if (b[0] == (byte)'B' && b[1] == (byte)'M' && b[7] == 0)
                {
                    type = (BitArray.Type)Enum.ToObject(typeof(BitArray.Type), b[6]);
                    var c = Helper.ToInt32(b, 2);
                    var buf = new byte[c * 4];
                    bmp.Read(buf, 0, c * 4);
                    for (var i = 0; i < c; i++)
                    {
                        ar.Add((uint)Helper.ToInt32(buf, i * 4));
                    }
                }
            }
            bc = new BitArray(type, ar.ToArray());

            return bc;
        }

        //#pragma warning disable 642
        private void CheckInternalOP()
        {
            if (_stopOperations)
                lock (_oplock) { } // yes! this is good
            Interlocked.Increment(ref _workingCount);
        }
        //#pragma warning restore 642

        private void Done()
        {
            Interlocked.Decrement(ref _workingCount);
        }
        #endregion


    }
}
