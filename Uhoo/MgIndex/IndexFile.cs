using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using UhooIndexer.FastBinaryJson;
using UhooIndexer.Utilities;

namespace UhooIndexer.MgIndex
{
    internal class IndexFile<T>
    {
        FileStream _file;
        private byte[] _fileHeader = new byte[] {
            (byte)'M', (byte)'G', (byte)'I',
            0,               // 3 = [keysize]   max 255
            0,0,             // 4 = [node size] max 65536
            0,0,0,0,         // 6 = [root page num]
            0,               // 10 = Index file type : 0=mgindex 1=mgindex+strings (key = firstallocblock)
            0,0,0,0          // 11 = last record number indexed 
            };

        private readonly byte[] _blockHeader = new byte[] { 
            (byte)'P',(byte)'A',(byte)'G',(byte)'E',
            0,               // 4 = [Flag] = 0=page 1=page list   
            0,0,             // 5 = [item count] 
            0,0,0,0,         // 7 = reserved               
            0,0,0,0          // 11 = [right page number] / [next page number]
        };

        internal byte MaxKeySize;
        internal ushort PageNodeCount = 5000;
        private int _lastPageNumber = 1; // 0 = page list
        private readonly int _pageLength;
        private readonly int _rowSize;
        private const bool AllowDups = true;
        private readonly BitmapIndex _bitmap;
        IGetBytes<T> _T;
        private readonly object _fileLock = new object();

        private readonly KeyStoreHighFrequency _strings;
        private bool _externalStrings;

        public IndexFile(string filename, byte maxKeySize)//, ushort pageNodeCount)
        {
            _T = RDBDataType<T>.ByteHandler();
            if (typeof(T) == typeof(string) )//&& Global.EnableOptimizedStringIndex)
            {
                _externalStrings = true;
                MaxKeySize = 4;// blocknum:int
            } 
            else
                MaxKeySize = maxKeySize;

            PageNodeCount = Global.PageItemCount;// pageNodeCount;
            _rowSize = (MaxKeySize + 1 + 4 + 4);

            string path = Path.GetDirectoryName(filename);
            Directory.CreateDirectory(path);
            if (File.Exists(filename))
            {
                // if file exists open and read header
                _file = File.Open(filename, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
                ReadFileHeader();
                if (_externalStrings == false)// if the file says different
                {
                    _rowSize = (MaxKeySize + 1 + 4 + 4);
                }
                // compute last page number from file length 
                _pageLength = (_blockHeader.Length + _rowSize * (PageNodeCount));
                _lastPageNumber = (int)((_file.Length - _fileHeader.Length) / _pageLength);
            }
            else
            {
                // else create new file
                _file = File.Open(filename, FileMode.Create, FileAccess.ReadWrite, FileShare.ReadWrite);

                _pageLength = (_blockHeader.Length + _rowSize * (PageNodeCount));

                CreateFileHeader(0);

                _lastPageNumber = (int)((_file.Length - _fileHeader.Length) / _pageLength);
            }
            if (_externalStrings)
            {
                _strings = new KeyStoreHighFrequency(path, Path.GetFileNameWithoutExtension(filename) + ".strings");
            }
            if (_lastPageNumber == 0)
                _lastPageNumber = 1;
            // bitmap duplicates 
            if (AllowDups)
                _bitmap = new BitmapIndex(Path.GetDirectoryName(filename), Path.GetFileNameWithoutExtension(filename));
        }

        #region [  C o m m o n  ]
        public void SetBitmapDuplicate(int bitmaprec, int rec)
        {
            _bitmap.SetDuplicate(bitmaprec, rec);
        }

        public int GetBitmapDuplaicateFreeRecordNumber()
        {
            return _bitmap.GetFreeRecordNumber();
        }

        public IEnumerable<int> GetDuplicatesRecordNumbers(int recno)
        {
            return GetDuplicateBitmap(recno).GetBitIndexes();
        }

        public BitArray GetDuplicateBitmap(int recno)
        {
            return _bitmap.GetBitmap(recno);
        }

        private byte[] CreateBlockHeader(byte type, ushort itemcount, int rightpagenumber)
        {
            byte[] block = new byte[_blockHeader.Length];
            Array.Copy(_blockHeader, block, block.Length);
            block[4] = type;
            byte[] b = Helper.GetBytes(itemcount, false);
            Buffer.BlockCopy(b, 0, block, 5, 2);
            b = Helper.GetBytes(rightpagenumber, false);
            Buffer.BlockCopy(b, 0, block, 11, 4);
            return block;
        }

        private void CreateFileHeader(int rowsindexed)
        {
            lock (_fileLock)
            {
                // max key size
                byte[] b = Helper.GetBytes(MaxKeySize, false);
                Buffer.BlockCopy(b, 0, _fileHeader, 3, 1);
                // page node count
                b = Helper.GetBytes(PageNodeCount, false);
                Buffer.BlockCopy(b, 0, _fileHeader, 4, 2);
                b = Helper.GetBytes(rowsindexed, false);
                Buffer.BlockCopy(b, 0, _fileHeader, 11, 4);

                if (_externalStrings)
                    _fileHeader[10] = 1;

                _file.Seek(0L, SeekOrigin.Begin);
                _file.Write(_fileHeader, 0, _fileHeader.Length);
                if (rowsindexed == 0)
                {
                    byte[] pagezero = new byte[_pageLength];
                    byte[] block = CreateBlockHeader(1, 0, -1);
                    Buffer.BlockCopy(block, 0, pagezero, 0, block.Length);
                    _file.Write(pagezero, 0, _pageLength);
                }
                _file.Flush();
            }
        }

        private bool ReadFileHeader()
        {
            _file.Seek(0L, SeekOrigin.Begin);
            byte[] b = new byte[_fileHeader.Length];
            _file.Read(b, 0, _fileHeader.Length);

            if (b[0] == _fileHeader[0] && b[1] == _fileHeader[1] && b[2] == _fileHeader[2]) // header
            {
                byte maxks = b[3];
                ushort nodes = (ushort)Helper.ToInt16(b, 4);
                int root = Helper.ToInt32(b, 6);
                MaxKeySize = maxks;
                PageNodeCount = nodes;
                _fileHeader = b;
                if (b[10] == 0)
                    _externalStrings = false;
            }

            return false;
        }

        public int GetNewPageNumber()
        {
            return Interlocked.Increment(ref _lastPageNumber); //_LastPageNumber++;
        }

        private void SeekPage(int pnum)
        {
            long offset = _fileHeader.Length;
            offset += (long)pnum * _pageLength;
            if (offset > _file.Length)
                CreateBlankPages(pnum);

            _file.Seek(offset, SeekOrigin.Begin);
        }

        private void CreateBlankPages(int pnum)
        {
            // create space
            byte[] b = new byte[_pageLength];
            _file.Seek(0L, SeekOrigin.Current);
            for (int i = pnum; i < _lastPageNumber; i++)
                _file.Write(b, 0, b.Length);

            _file.Flush();
        }

        public void FreeMemory()
        {
            if (AllowDups)
                _bitmap.FreeMemory();
        }

        public void Shutdown()
        {
            Logging.WriteLog("Shutdown IndexFile", Logging.LogType.Information, Logging.LogCaller.IndexFile);
            if (_externalStrings)
                _strings.Shutdown();

            if (_file != null)
            {
                _file.Flush();
                _file.Close();
            }
            _file = null;
            if (AllowDups)
            {
                _bitmap.Commit(Global.FreeBitmapMemoryOnSave);
                _bitmap.Shutdown();
            }
        }

        #endregion

        #region [  P a g e s ]

        public void GetPageList(List<int> PageListDiskPages, SafeSortedList<T, PageInfo> PageList, out int lastIndexedRow)
        {
            lastIndexedRow = Helper.ToInt32(_fileHeader, 11);
            // load page list
            PageListDiskPages.Add(0); // first page list
            int nextpage = LoadPageListData(0, PageList);
            while (nextpage != -1)
            {
                nextpage = LoadPageListData(nextpage, PageList);

                if (nextpage != -1)
                    PageListDiskPages.Add(nextpage);
            }
        }

        private int LoadPageListData(int page, SafeSortedList<T, PageInfo> PageList)
        {
            lock (_fileLock)
            {
                // load page list data
                int nextpage = -1;
                SeekPage(page);
                byte[] b = new byte[_pageLength];
                _file.Read(b, 0, _pageLength);

                if (b[0] == _blockHeader[0] && b[1] == _blockHeader[1] && b[2] == _blockHeader[2] && b[3] == _blockHeader[3])
                {
                    short count = Helper.ToInt16(b, 5);
                    if (count > PageNodeCount)
                        throw new Exception("Count > node size");
                    nextpage = Helper.ToInt32(b, 11);
                    int index = _blockHeader.Length;

                    for (int i = 0; i < count; i++)
                    {
                        int idx = index + _rowSize * i;
                        byte ks = b[idx];
                        T key = _T.GetObject(b, idx + 1, ks);
                        int pagenum = Helper.ToInt32(b, idx + 1 + MaxKeySize);
                        // add counts
                        int unique = Helper.ToInt32(b, idx + 1 + MaxKeySize + 4);
                        // FEATURE : add dup count
                        PageList.Add(key, new PageInfo(pagenum, unique, 0));
                    }
                }
                else
                    throw new Exception("Page List header is invalid");

                return nextpage;
            }
        }

        internal void SavePage(Page<T> node)
        {
            lock (_fileLock)
            {
                int pnum = node.DiskPageNumber;
                if (pnum > _lastPageNumber)
                    throw new Exception("should not be here: page out of bounds");

                SeekPage(pnum);
                byte[] page = new byte[_pageLength];
                byte[] blockheader = CreateBlockHeader(0, (ushort)node.Tree.Count, node.RightPageNumber);
                Buffer.BlockCopy(blockheader, 0, page, 0, blockheader.Length);

                int index = blockheader.Length;
                int i = 0;
                byte[] b = null;
                T[] keys = node.Tree.Keys();
                Array.Sort(keys); // sort keys on save for read performance
                int blocknum = 0;
                if (_externalStrings)
                {
                    // free old blocks
                    if (node.AllocBlocks != null)
                        _strings.FreeBlocks(node.AllocBlocks);
                    blocknum = _strings.SaveData(node.DiskPageNumber.ToString(), FastBinaryJson.BinaryJson.ToBjson(keys));
                }
                // node children
                foreach (var kp in keys)
                {
                    var val = node.Tree[kp];
                    int idx = index + _rowSize * i;
                    // key bytes
                    byte[] kk;
                    byte size;
                    if (_externalStrings == false)
                    {
                        kk = _T.GetBytes(kp);
                        size = (byte)kk.Length;
                        if (size > MaxKeySize)
                            size = MaxKeySize;
                    }
                    else
                    {
                        kk = new byte[4];
                        Buffer.BlockCopy(Helper.GetBytes(blocknum, false), 0, kk, 0, 4);
                        size = 4;
                    }
                    // key size = 1 byte
                    page[idx] = size;
                    Buffer.BlockCopy(kk, 0, page, idx + 1, page[idx]);
                    // offset = 4 bytes
                    b = Helper.GetBytes(val.RecordNumber, false);
                    Buffer.BlockCopy(b, 0, page, idx + 1 + MaxKeySize, b.Length);
                    // duplicatepage = 4 bytes
                    b = Helper.GetBytes(val.DuplicateBitmapNumber, false);
                    Buffer.BlockCopy(b, 0, page, idx + 1 + MaxKeySize + 4, b.Length);
                    i++;
                }
                _file.Write(page, 0, page.Length);
            }
        }

        public Page<T> LoadPageFromPageNumber(int number)
        {
            lock (_fileLock)
            {
                SeekPage(number);
                byte[] b = new byte[_pageLength];
                _file.Read(b, 0, _pageLength);

                if (b[0] == _blockHeader[0] && b[1] == _blockHeader[1] && b[2] == _blockHeader[2] && b[3] == _blockHeader[3])
                {
                    // create node here
                    Page<T> page = new Page<T>();

                    short count = Helper.ToInt16(b, 5);
                    if (count > PageNodeCount)
                        throw new Exception("Count > node size");
                    page.DiskPageNumber = number;
                    page.RightPageNumber = Helper.ToInt32(b, 11);
                    int index = _blockHeader.Length;
                    object[] keys = null;

                    for (int i = 0; i < count; i++)
                    {
                        int idx = index + _rowSize * i;
                        byte ks = b[idx];
                        T key;
                        if (_externalStrings == false)
                            key = _T.GetObject(b, idx + 1, ks);
                        else
                        {
                            if (keys == null)
                            {
                                int blknum = Helper.ToInt32(b, idx + 1, false);
                                byte[] bb = _strings.GetData(blknum, page.AllocBlocks);
                                keys = (object[])BinaryJson.ToObject(bb);
                            }
                            key = (T)keys[i];
                        }
                        int offset = Helper.ToInt32(b, idx + 1 + MaxKeySize);
                        int duppage = Helper.ToInt32(b, idx + 1 + MaxKeySize + 4);
                        page.Tree.Add(key, new KeyInfo(offset, duppage));
                    }
                    return page;
                }
                else
                    throw new Exception("Page read error header invalid, number = " + number);
            }
        }
        #endregion

        internal void SavePageList(SafeSortedList<T, PageInfo> _pages, List<int> diskpages)
        {
            lock (_fileLock)
            {
                // save page list
                int c = (_pages.Count / Global.PageItemCount) + 1;
                // allocate pages needed 
                while (c > diskpages.Count)
                    diskpages.Add(GetNewPageNumber());

                byte[] page = new byte[_pageLength];

                for (int i = 0; i < (diskpages.Count - 1); i++)
                {
                    byte[] block = CreateBlockHeader(1, Global.PageItemCount, diskpages[i + 1]);
                    Buffer.BlockCopy(block, 0, page, 0, block.Length);

                    for (int j = 0; j < Global.PageItemCount; j++)
                        CreatePageListData(_pages, i * Global.PageItemCount, block.Length, j, page);

                    SeekPage(diskpages[i]);
                    _file.Write(page, 0, page.Length);
                }

                c = _pages.Count % Global.PageItemCount;
                byte[] lastblock = CreateBlockHeader(1, (ushort)c, -1);
                Buffer.BlockCopy(lastblock, 0, page, 0, lastblock.Length);
                int lastoffset = (_pages.Count / Global.PageItemCount) * Global.PageItemCount;

                for (int j = 0; j < c; j++)
                    CreatePageListData(_pages, lastoffset, lastblock.Length, j, page);

                SeekPage(diskpages[diskpages.Count - 1]);
                _file.Write(page, 0, page.Length);
            }
        }

        private void CreatePageListData(SafeSortedList<T, PageInfo> _pages, int offset, int index, int counter, byte[] page)
        {
            int idx = index + _rowSize * counter;
            // key bytes
            byte[] kk = _T.GetBytes(_pages.GetKey(counter + offset));
            byte size = (byte)kk.Length;
            if (size > MaxKeySize)
                size = MaxKeySize;
            // key size = 1 byte
            page[idx] = size;
            Buffer.BlockCopy(kk, 0, page, idx + 1, page[idx]);
            // offset = 4 bytes
            byte[] b = Helper.GetBytes(_pages.GetValue(offset + counter).PageNumber, false);
            Buffer.BlockCopy(b, 0, page, idx + 1 + MaxKeySize, b.Length);
            // add counts 
            b = Helper.GetBytes(_pages.GetValue(offset + counter).UniqueCount, false);
            Buffer.BlockCopy(b, 0, page, idx + 1 + MaxKeySize + 4, b.Length);
            // FEATURE : add dup counts
        }

        internal void SaveLastRecordNumber(int recnum)
        {
            // save the last record number indexed to the header
            CreateFileHeader(recnum);
        }

        internal void BitmapFlush()
        {
            if (AllowDups)
                _bitmap.Commit(Global.FreeBitmapMemoryOnSave);
        }
    }
}