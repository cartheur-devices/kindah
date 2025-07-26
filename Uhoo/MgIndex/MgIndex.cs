using System;
using System.Collections.Generic;
using System.IO;
using UhooIndexer.Utilities;

namespace UhooIndexer.MgIndex
{  
    /// <summary>
    /// FEATURE : change back to class for count access for query caching.
    /// </summary>
    internal struct PageInfo
    {
        public PageInfo(int pagenum, int uniquecount, int duplicatecount)
        {
            PageNumber = pagenum;
            UniqueCount = uniquecount;
            DuplicateCount = duplicatecount;
        }
        public int PageNumber;
        public int UniqueCount;
        public int DuplicateCount;
    }

    internal struct KeyInfo
    {
        public KeyInfo(int recnum)
        {
            RecordNumber = recnum;
            DuplicateBitmapNumber = -1;
        }
        public KeyInfo(int recnum, int bitmaprec)
        {
            RecordNumber = recnum;
            DuplicateBitmapNumber = bitmaprec;
        }
        public int RecordNumber;
        public int DuplicateBitmapNumber;
    }

    internal class Page<T>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="Page{T}"/> class. A kludge so the compiler doesn't complain.
        /// </summary>
        public Page()
        {
            DiskPageNumber = -1;
            RightPageNumber = -1;
            Tree = new SafeDictionary<T, KeyInfo>(Global.PageItemCount);
            IsDirty = false;
            FirstKey = default(T);
        }
        public int DiskPageNumber;
        public int RightPageNumber;
        public T FirstKey;
        public bool IsDirty;
        public SafeDictionary<T, KeyInfo> Tree;
        public List<int> AllocBlocks = null; // for string keys in HF key store
    }

    internal class MgIndex<T> where T : IComparable<T>
    {
        private readonly SafeSortedList<T, PageInfo> _pageList = new SafeSortedList<T, PageInfo>();
        //private SafeDictionary<int, Page<T>> _cache = new SafeDictionary<int, Page<T>>();
        private readonly SafeSortedList<int, Page<T>> _cache = new SafeSortedList<int, Page<T>>();
        private readonly List<int> _pageListDiskPages = new List<int>();
        private readonly IndexFile<T> _index;
        private readonly bool _allowDuplicates = true;
        private int _lastIndexedRecordNumber = 0;
        //private int _maxPageItems = 0;
        private readonly object _setlock = new object();

        public MgIndex(string path, string filename, byte keysize, /*ushort maxcount,*/ bool allowdups)
        {
            _allowDuplicates = allowdups;
            _index = new IndexFile<T>(path + Path.DirectorySeparatorChar + filename, keysize);//, maxcount);
            //_maxPageItems = maxcount;
            // load page list
            _index.GetPageList(_pageListDiskPages, _pageList, out _lastIndexedRecordNumber);
            if (_pageList.Count == 0)
            {
                var page = new Page<T>
                {
                    FirstKey = (T) RDBDataType<T>.GetEmpty(),
                    DiskPageNumber = _index.GetNewPageNumber(),
                    IsDirty = true
                };
                _pageList.Add(page.FirstKey, new PageInfo(page.DiskPageNumber, 0, 0));
                _cache.Add(page.DiskPageNumber, page);
            }
        }

        public int GetLastIndexedRecordNumber()
        {
            return _lastIndexedRecordNumber;
        }
        public BitArray Query(T from, T to, int maxsize)
        {
            // TODO : add BETWEEN code here
            var temp = default(T);
            if (from.CompareTo(to) > 0) // check values order
            {
                temp = from;
                from = to;
                to = temp;
            }
            // find first page and do > than
            var found = false;
            var startpos = FindPageOrLowerPosition(from, ref found);

            // find last page and do < than
            var endpos = FindPageOrLowerPosition(to, ref found);

            // do all pages in between

            return new BitArray();
        }
        public BitArray Query(RdbExpression exp, T from, int maxsize)
        {
            var key = from;
            if (exp == RdbExpression.Equal || exp == RdbExpression.NotEqual)
                return doEqualOp(exp, key, maxsize);

            // FEATURE : optimize complement search if page count less for the complement pages

            if (exp == RdbExpression.Less || exp == RdbExpression.LessEqual)
            {
                return doLessOp(exp, key);
            }
            else if (exp == RdbExpression.Greater || exp == RdbExpression.GreaterEqual)
            {
                return doMoreOp(exp, key);
            }

            return new BitArray(); // blank results 
        }
        public void Set(T key, int val)
        {
            lock (_setlock)
            {
                PageInfo pi;
                var page = LoadPage(key, out pi);

                KeyInfo ki;
                if (page.Tree.TryGetValue(key, out ki))
                {
                    // item exists
                    if (_allowDuplicates)
                    {
                        SaveDuplicate(key, ref ki);
                        // set current record in the bitmap also
                        _index.SetBitmapDuplicate(ki.DuplicateBitmapNumber, val);
                    }
                    ki.RecordNumber = val;
                    page.Tree[key] = ki; // structs need resetting
                }
                else
                {
                    // new item 
                    ki = new KeyInfo(val);
                    if (_allowDuplicates)
                        SaveDuplicate(key, ref ki);
                    pi.UniqueCount++;
                    page.Tree.Add(key, ki);
                }

                if (page.Tree.Count > Global.PageItemCount)
                    SplitPage(page);

                _lastIndexedRecordNumber = val;
                page.IsDirty = true;
            }
        }
        public bool Get(T key, out int val)
        {
            val = -1;
            PageInfo pi;
            var page = LoadPage(key, out pi);
            KeyInfo ki;
            var ret = page.Tree.TryGetValue(key, out ki);
            if (ret)
                val = ki.RecordNumber;
            return ret;
        }
        public void SaveIndex()
        {
            //_log.Debug("Total split time (s) = " + _totalsplits);
            //_log.Debug("Total pages = " + _pageList.Count);
            var keys = _cache.Keys();
            Array.Sort(keys);
            // save index to disk
            foreach (var i in keys)
            {
                var p = _cache[i];
                if (p.IsDirty)
                {
                    _index.SavePage(p);
                    p.IsDirty = false;
                }
            }
            _index.SavePageList(_pageList, _pageListDiskPages);
            _index.BitmapFlush();
        }
        public void Shutdown()
        {
            SaveIndex();
            // save page list
            //_index.SavePageList(_pageList, _pageListDiskPages);
            // shutdown
            _index.Shutdown();
        }
        public void FreeMemory()
        {
            _index.FreeMemory();
            try
            {
                var free = new List<int>();
                foreach (var c in _cache)
                {
                    if (c.Value.IsDirty == false)
                        free.Add(c.Key);
                }
                Logging.WriteLog("releasing page count = " + free.Count + " out of " + _cache.Count, Logging.LogType.Information, Logging.LogCaller.MgIndex);
                foreach (var i in free)
                    _cache.Remove(i);
            }
            catch { }
        }
        public IEnumerable<int> GetDuplicates(T key)
        {
            PageInfo pi;
            var page = LoadPage(key, out pi);
            KeyInfo ki;
            var ret = page.Tree.TryGetValue(key, out ki);
            if (ret)
                // get duplicates
                if (ki.DuplicateBitmapNumber != -1)
                    return _index.GetDuplicatesRecordNumbers(ki.DuplicateBitmapNumber);

            return new List<int>();
        }

        public void SaveLastRecordNumber(int recnum)
        {
            _index.SaveLastRecordNumber(recnum);
        }
        public bool RemoveKey(T key)
        {
            PageInfo pi;
            var page = LoadPage(key, out pi);
            var b = page.Tree.Remove(key);
            // TODO : reset the first key for page ??
            if (b)
            {
                pi.UniqueCount--;
                // FEATURE : decrease dup count
            }
            page.IsDirty = true;
            return b;
        }

        #region [  P R I V A T E  ]
        private BitArray doMoreOp(RdbExpression exp, T key)
        {
            var found = false;
            var pos = FindPageOrLowerPosition(key, ref found);
            var result = new BitArray();
            if (pos < _pageList.Count)
            {
                // all the pages after
                for (var i = pos + 1; i < _pageList.Count; i++)
                    doPageOperation(ref result, i);
            }
            // key page
            var page = LoadPage(_pageList.GetValue(pos).PageNumber);
            var keys = page.Tree.Keys();
            Array.Sort(keys);

            // find better start position rather than 0
            pos = Array.IndexOf<T>(keys, key);
            if (pos == -1) pos = 0;

            for (var i = pos; i < keys.Length; i++)
            {
                var k = keys[i];
                var bn = page.Tree[k].DuplicateBitmapNumber;

                if (k.CompareTo(key) > 0)
                    result = result.Or(_index.GetDuplicateBitmap(bn));

                if (exp == RdbExpression.GreaterEqual && k.CompareTo(key) == 0)
                    result = result.Or(_index.GetDuplicateBitmap(bn));
            }
            return result;
        }

        private BitArray doLessOp(RdbExpression exp, T key)
        {
            var found = false;
            var pos = FindPageOrLowerPosition(key, ref found);
            var result = new BitArray();
            if (pos > 0)
            {
                // all the pages before
                for (var i = 0; i < pos - 1; i++)
                    doPageOperation(ref result, i);
            }
            // key page
            var page = LoadPage(_pageList.GetValue(pos).PageNumber);
            var keys = page.Tree.Keys();
            Array.Sort(keys);
            for (var i = 0; i < keys.Length; i++)
            {
                var k = keys[i];
                if (k.CompareTo(key) > 0)
                    break;
                var bn = page.Tree[k].DuplicateBitmapNumber;

                if (k.CompareTo(key) < 0)
                    result = result.Or(_index.GetDuplicateBitmap(bn));

                if (exp == RdbExpression.LessEqual && k.CompareTo(key) == 0)
                    result = result.Or(_index.GetDuplicateBitmap(bn));
            }
            return result;
        }

        private BitArray doEqualOp(RdbExpression exp, T key, int maxsize)
        {
            PageInfo pi;
            var page = LoadPage(key, out pi);
            KeyInfo k;
            if (page.Tree.TryGetValue(key, out k))
            {
                var bn = k.DuplicateBitmapNumber;

                if (exp == RdbExpression.Equal)
                    return _index.GetDuplicateBitmap(bn);
                else
                    return _index.GetDuplicateBitmap(bn).Not(maxsize);
            }
            else
            {
                if (exp == RdbExpression.NotEqual)
                    return new BitArray().Not(maxsize);
                else
                    return new BitArray();
            }
        }

        private void doPageOperation(ref BitArray res, int pageidx)
        {
            var page = LoadPage(_pageList.GetValue(pageidx).PageNumber);
            var keys = page.Tree.Keys(); // avoid sync issues
            foreach (var k in keys)
            {
                var bn = page.Tree[k].DuplicateBitmapNumber;

                res = res.Or(_index.GetDuplicateBitmap(bn));
            }
        }

        private double _totalsplits = 0;
        private void SplitPage(Page<T> page)
        {
            // split the page
            var dt = FastDateTime.Now;

            var newpage = new Page<T>();
            newpage.DiskPageNumber = _index.GetNewPageNumber();
            newpage.RightPageNumber = page.RightPageNumber;
            newpage.IsDirty = true;
            page.RightPageNumber = newpage.DiskPageNumber;
            // get and sort keys
            var keys = page.Tree.Keys();
            Array.Sort<T>(keys);
            // copy data to new 
            for (var i = keys.Length / 2; i < keys.Length; i++)
            {
                newpage.Tree.Add(keys[i], page.Tree[keys[i]]);
                // remove from old page
                page.Tree.Remove(keys[i]);
            }
            // set the first key
            newpage.FirstKey = keys[keys.Length / 2];
            // set the first key refs
            _pageList.Remove(page.FirstKey);
            _pageList.Remove(keys[0]);
            // dup counts
            _pageList.Add(keys[0], new PageInfo(page.DiskPageNumber, page.Tree.Count, 0));
            page.FirstKey = keys[0];
            // FEATURE : dup counts
            _pageList.Add(newpage.FirstKey, new PageInfo(newpage.DiskPageNumber, newpage.Tree.Count, 0));
            _cache.Add(newpage.DiskPageNumber, newpage);

            _totalsplits += FastDateTime.Now.Subtract(dt).TotalSeconds;
        }

        private Page<T> LoadPage(T key, out PageInfo pageinfo)
        {
            var pagenum = -1;
            // find page in list of pages

            var found = false;
            var pos = 0;
            if (key != null)
                pos = FindPageOrLowerPosition(key, ref found);
            pageinfo = _pageList.GetValue(pos);
            pagenum = pageinfo.PageNumber;

            Page<T> page;
            if (_cache.TryGetValue(pagenum, out page) == false)
            {
                //load page from disk
                page = _index.LoadPageFromPageNumber(pagenum);
                _cache.Add(pagenum, page);
            }
            return page;
        }

        private Page<T> LoadPage(int pagenum)
        {
            Page<T> page;
            if (_cache.TryGetValue(pagenum, out page) == false)
            {
                //load page from disk
                page = _index.LoadPageFromPageNumber(pagenum);
                _cache.Add(pagenum, page);
            }
            return page;
        }

        private void SaveDuplicate(T key, ref KeyInfo ki)
        {
            if (ki.DuplicateBitmapNumber == -1)
                ki.DuplicateBitmapNumber = _index.GetBitmapDuplaicateFreeRecordNumber();

            _index.SetBitmapDuplicate(ki.DuplicateBitmapNumber, ki.RecordNumber);
        }

        private int FindPageOrLowerPosition(T key, ref bool found)
        {
            if (_pageList.Count == 0)
                return 0;
            // binary search
            var lastlower = 0;
            var first = 0;
            var last = _pageList.Count - 1;
            var mid = 0;
            while (first <= last)
            {
                mid = (first + last) >> 1;
                var k = _pageList.GetKey(mid);
                var compare = k.CompareTo(key);
                if (compare < 0)
                {
                    lastlower = mid;
                    first = mid + 1;
                }
                if (compare == 0)
                {
                    found = true;
                    return mid;
                }
                if (compare > 0)
                {
                    last = mid - 1;
                }
            }

            return lastlower;
        }
        #endregion

        internal object[] GetKeys()
        {
            var keys = new List<object>();
            for (var i = 0; i < _pageList.Count; i++)
            {
                var page = LoadPage(_pageList.GetValue(i).PageNumber);
                foreach (var k in page.Tree.Keys())
                    keys.Add(k);
            }
            return keys.ToArray();
        }
        internal int Count()
        {
            var count = 0;
            for (var i = 0; i < _pageList.Count; i++)
            {
                var page = LoadPage(_pageList.GetValue(i).PageNumber);
                //foreach (var k in page.tree.Keys())
                //    count++;
                count += page.Tree.Count;
            }
            return count;
        }
    }
}
