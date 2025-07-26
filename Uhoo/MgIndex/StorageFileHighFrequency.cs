using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading;

namespace UhooIndexer.MgIndex
{
    // high frequency storage file with overwrite old values
    internal class StorageFileHighFrequency
    {
        FileStream _datawrite;
        BitArray _freeList;
        readonly Action<BitArray> _savefreeList;
        readonly Func<BitArray> _readfreeList;

        private readonly string _filename = "";
        private object _readlock = new object();
        //ILog _log = LogManager.GetLogger(typeof(StorageFileHF));

        // **** change this if storage format changed ****
        internal static int CurrentVersion = 1;
        int _lastBlockNumber = 0;
        private ushort _blocksize = 4096;
        private readonly string _path = "";
        private readonly string _s = Path.DirectorySeparatorChar.ToString(CultureInfo.InvariantCulture);

        public static byte[] FileHeader = { (byte)'M', (byte)'G', (byte)'H', (byte)'F',
                                              0,   // 4 -- storage file version number,
                                              0,2, // 5,6 -- block size ushort low, hi
                                              1    // 7 -- key type 0 = guid, 1 = string
                                           };

        public StorageFileHighFrequency(string filename, ushort blocksize) : this(filename, blocksize, null, null)
        {
        }

        // used for bitmapindexhf
        public StorageFileHighFrequency(string filename, ushort blocksize, Func<BitArray> readfreelist, Action<BitArray> savefreelist)
        {
            _savefreeList = savefreelist;
            _readfreeList = readfreelist;
            _path = Path.GetDirectoryName(filename);
            if (_path.EndsWith(_s) == false) _path += _s;
            _filename = Path.GetFileNameWithoutExtension(filename);

            Initialize(filename, blocksize);
        }

        public void Shutdown()
        {
            // write free list 
            if (_savefreeList != null)
                _savefreeList(_freeList);
            else
                WriteFreeListBMPFile(_path + _filename + ".free");
            FlushClose(_datawrite);
            _datawrite = null;
        }

        public ushort GetBlockSize()
        {
            return _blocksize;
        }

        internal void FreeBlocks(List<int> list)
        {
            list.ForEach(x => _freeList.Set(x, true));
        }

        internal byte[] ReadBlock(int blocknumber)
        {
            SeekBlock(blocknumber);
            byte[] data = new byte[_blocksize];
            _datawrite.Read(data, 0, _blocksize);

            return data;
        }

        internal byte[] ReadBlockBytes(int blocknumber, int bytes)
        {
            SeekBlock(blocknumber);
            byte[] data = new byte[bytes];
            _datawrite.Read(data, 0, bytes);

            return data;
        }

        internal int GetFreeBlockNumber()
        {
            // get the first free block or append to the end
            if (_freeList.CountOnes() > 0)
            {
                int i = _freeList.GetFirst();
                _freeList.Set(i, false);
                return i;
            }
            else
                return Interlocked.Increment(ref _lastBlockNumber);//++;
        }

        internal void Initialize()
        {
            if (_readfreeList != null)
                _freeList = _readfreeList();
            else
            {
                _freeList = new BitArray();
                if (File.Exists(_path + _filename + ".free"))
                {
                    ReadFreeListBMPFile(_path + _filename + ".free");
                    // delete file so if failure no big deal on restart
                    File.Delete(_path + _filename + ".free");
                }
            }
        }

        internal void SeekBlock(int blocknumber)
        {
            long offset = FileHeader.Length + (long)blocknumber * _blocksize;
            _datawrite.Seek(offset, SeekOrigin.Begin);// wiil seek past the end of file on fs.Write will zero the difference
        }

        internal void WriteBlockBytes(byte[] data, int start, int len)
        {
            _datawrite.Write(data, start, len);
        }

        #region [ private / internal  ]

        private void WriteFreeListBMPFile(string filename)
        {
            if (_freeList != null)
            {
                BitArray.Type t;
                uint[] ints = _freeList.GetCompressed(out t);
                MemoryStream ms = new MemoryStream();
                BinaryWriter bw = new BinaryWriter(ms);
                bw.Write((byte)t);// write new format with the data type byte
                foreach (var i in ints)
                {
                    bw.Write(i);
                }
                File.WriteAllBytes(filename, ms.ToArray());
            }
        }

        private void ReadFreeListBMPFile(string filename)
        {
            byte[] b = File.ReadAllBytes(filename);
            BitArray.Type t = BitArray.Type.Wah;
            int j = 0;
            if (b.Length % 4 > 0) // new format with the data type byte
            {
                t = (BitArray.Type)Enum.ToObject(typeof(BitArray.Type), b[0]);
                j = 1;
            }
            List<uint> ints = new List<uint>();
            for (int i = 0; i < b.Length / 4; i++)
            {
                ints.Add((uint)Helper.ToInt32(b, (i * 4) + j));
            }
            _freeList = new BitArray(t, ints.ToArray());
        }

        private void Initialize(string filename, ushort blocksize)
        {
            if (File.Exists(filename) == false)
                _datawrite = new FileStream(filename, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.ReadWrite);
            else
                _datawrite = new FileStream(filename, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);

            if (_datawrite.Length == 0)
            {
                CreateFileHeader(blocksize);
                // new file
                _datawrite.Write(FileHeader, 0, FileHeader.Length);
                _datawrite.Flush();
            }
            else
            {
                ReadFileHeader();
                _lastBlockNumber = (int)((_datawrite.Length - FileHeader.Length) / _blocksize);
                _lastBlockNumber++;
            }
            //if (_readfreeList != null)
            //    _freeList = _readfreeList();
            //else
            //{
            //    _freeList = new WAHBitArray();
            //    if (File.Exists(_Path + _filename + ".free"))
            //    {
            //        ReadFreeListBMPFile(_Path + _filename + ".free");
            //        // delete file so if failure no big deal on restart
            //        File.Delete(_Path + _filename + ".free");
            //    }
            //}
        }

        private void ReadFileHeader()
        {
            // set _blockize
            _datawrite.Seek(0L, SeekOrigin.Begin);
            byte[] hdr = new byte[FileHeader.Length];
            _datawrite.Read(hdr, 0, FileHeader.Length);

            _blocksize = 0;
            _blocksize = (ushort)(hdr[5] + hdr[6] << 8);
        }

        private void CreateFileHeader(int blocksize)
        {
            // add version number
            FileHeader[4] = (byte)CurrentVersion;
            // block size
            FileHeader[5] = (byte)(blocksize & 0xff);
            FileHeader[6] = (byte)(blocksize >> 8);
            _blocksize = (ushort)blocksize;
        }

        private void FlushClose(FileStream st)
        {
            if (st != null)
            {
                st.Flush(true);
                st.Close();
            }
        }
        #endregion

        internal int NumberofBlocks()
        {
            return (int)((_datawrite.Length / _blocksize) + 1);
        }

        internal void FreeBlock(int i)
        {
            _freeList.Set(i, true);
        }
    }
}
