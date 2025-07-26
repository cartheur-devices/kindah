using System;
using System.Collections.Generic;
using System.IO;

namespace UhooIndexer.MgIndex
{
    #region [  TypeIndexes  ]
    internal class TypeIndexes<T> : MgIndex<T>, IIndex where T : IComparable<T>
    {
        public TypeIndexes(string path, string filename, byte keysize)
            : base(path, filename + ".mgidx", keysize, /*Global.PageItemCount,*/ true)
        {

        }

        public void Set(object key, int recnum)
        {
            if (key == null) return; // FEATURE : index null values ??

            base.Set((T)key, recnum);
        }

        public BitArray Query(RdbExpression ex, object from, int maxsize)
        {
            T f = default(T);
            if (typeof(T).Equals(from.GetType()) == false)
                f = Converter(from);
            else
                f = (T)from;

            return base.Query(ex, f, maxsize);
        }

        private T Converter(object from)
        {
            if (typeof(T) == typeof(Guid))
            {
                object o = new Guid(from.ToString());
                return (T)o;
            }
            else
                return (T)Convert.ChangeType(from, typeof(T));
        }

        void IIndex.FreeMemory()
        {
            base.FreeMemory();
            base.SaveIndex();
        }

        void IIndex.Shutdown()
        {
            //base.SaveIndex();
            base.Shutdown();
        }

        object[] IIndex.GetKeys()
        {
            return base.GetKeys();
        }
        public BitArray Query(object fromkey, object tokey, int maxsize)
        {
            T f = default(T);
            if (typeof(T).Equals(fromkey.GetType()) == false)
                f = (T)Convert.ChangeType(fromkey, typeof(T));
            else
                f = (T)fromkey;

            T t = default(T);
            if (typeof(T).Equals(tokey.GetType()) == false)
                t = (T)Convert.ChangeType(tokey, typeof(T));
            else
                t = (T)tokey;

            return base.Query(f, t, maxsize);
        }
    }
    #endregion

    #region [  BoolIndex  ]
    internal class BoolIndex : IIndex
    {
        public BoolIndex(string path, string filename, string extension)
        {
            // create file
            _filename = filename + extension;
            //if (_filename.Contains(".") == false) _filename += ".deleted";
            _path = path;
            if (_path.EndsWith(Path.DirectorySeparatorChar.ToString()) == false)
                _path += Path.DirectorySeparatorChar.ToString();

            if (File.Exists(_path + _filename))
                ReadFile();
        }

        private BitArray _bits = new BitArray();
        private string _filename;
        private string _path;
        private object _lock = new object();
        //private bool _inMemory = false;

        public BitArray GetBits()
        {
            return _bits.Copy();
        }

        public void Set(object key, int recnum)
        {
            lock (_lock)
                if (key != null)
                    _bits.Set(recnum, (bool)key);
        }

        public BitArray Query(RdbExpression ex, object from, int maxsize)
        {
            lock (_lock)
            {
                bool b = (bool)from;
                if (b)
                    return _bits;
                else
                    return _bits.Not(maxsize);
            }
        }

        public void FreeMemory()
        {
            lock (_lock)
            {
                SaveIndex();
                _bits.FreeMemory();
                // free memory
                //_bits.FreeMemory();
                // save to disk
                //SaveIndex();
            }
        }

        public void Shutdown()
        {
            // shutdown
            //if (_inMemory == false)
            WriteFile();
        }

        public void SaveIndex()
        {
            //if (_inMemory == false)
            WriteFile();
        }

        public void InPlaceOR(BitArray left)
        {
            lock (_lock)
                _bits = _bits.Or(left);
        }

        private void WriteFile()
        {
            lock (_lock)
            {
                BitArray.Type t;
                uint[] ints = _bits.GetCompressed(out t);
                MemoryStream ms = new MemoryStream();
                BinaryWriter bw = new BinaryWriter(ms);
                bw.Write((byte)t);// write new format with the data type byte
                foreach (var i in ints)
                {
                    bw.Write(i);
                }
                bw.Flush();
                File.WriteAllBytes(_path + _filename, ms.ToArray());
            }
        }

        private void ReadFile()
        {
            byte[] b = File.ReadAllBytes(_path + _filename);
            MemoryStream ms = new MemoryStream(b);
            BinaryReader br = new BinaryReader(ms);
            BitArray.Type t = BitArray.Type.Wah;
            if (b.Length % 4 > 0) // new format with the data type byte
            {
                byte tb = br.ReadByte();
                t = (BitArray.Type)Enum.ToObject(typeof(BitArray.Type), tb);
            }
            List<uint> ints = new List<uint>();
            for (int i = 0; i < b.Length / 4; i++)
            {
                ints.Add((uint)br.ReadInt32());
            }
            _bits = new BitArray(t, ints.ToArray());
        }

        public BitArray Query(object fromkey, object tokey, int maxsize)
        {
            return Query(RdbExpression.Greater, fromkey, maxsize);
        }

        internal void FixSize(int size)
        {
            _bits.Length = size;
        }

        public object[] GetKeys()
        {
            return new object[] { true, false };
        }
    }
    #endregion

    #region [  FullTextIndex  ]
    internal class FullTextIndex : Uhoo, IIndex
    {
        public FullTextIndex(string IndexPath, string FileName, bool docmode, bool sortable)
            : base(IndexPath, FileName, docmode)
        {
            if (sortable)
            {
                _idx = new TypeIndexes<string>(IndexPath, FileName, Global.DefaultStringKeySize);
                _sortable = true;
            }
        }
        private bool _sortable = false;
        private IIndex _idx;

        public void Set(object key, int recnum)
        {
            base.Index(recnum, (string)key);
            if (_sortable)
                _idx.Set(key, recnum);
        }

        public BitArray Query(RdbExpression ex, object from, int maxsize)
        {
            return base.Query("" + from, maxsize);
        }

        public void SaveIndex()
        {
            base.Save();
            if (_sortable)
                _idx.SaveIndex();
        }

        public BitArray Query(object fromkey, object tokey, int maxsize)
        {
            return base.Query("" + fromkey, maxsize);
        }

        public object[] GetKeys()
        {
            if (_sortable)
                return _idx.GetKeys(); // support get keys 
            else
                return new object[] { };
        }
        void IIndex.FreeMemory()
        {
            base.FreeMemory();

            this.SaveIndex();
        }

        void IIndex.Shutdown()
        {
            this.SaveIndex();
            base.Shutdown();
            if (_sortable)
                _idx.Shutdown();
        }

    }
    #endregion

    #region [  EnumIndex  ]
    internal class EnumIndex<T> : MgIndex<string>, IIndex //where T : IComparable<T>
    {
        public EnumIndex(string path, string filename)
            : base(path, filename + ".mgidx", 30, /*Global.PageItemCount,*/ true)
        {

        }

        public void Set(object key, int recnum)
        {
            if (key == null) return; // FEATURE : index null values ??

            base.Set(key.ToString(), recnum);
        }

        public BitArray Query(RdbExpression ex, object from, int maxsize)
        {
            T f = default(T);
            if (typeof(T).Equals(from.GetType()) == false)
                f = Converter(from);
            else
                f = (T)from;

            return base.Query(ex, f.ToString(), maxsize);
        }

        private T Converter(object from)
        {
            if (typeof(T) == typeof(Guid))
            {
                object o = new Guid(from.ToString());
                return (T)o;
            }
            else
                return (T)Convert.ChangeType(from, typeof(T));
        }

        void IIndex.FreeMemory()
        {
            base.FreeMemory();
            base.SaveIndex();
        }

        void IIndex.Shutdown()
        {
            base.SaveIndex();
            base.Shutdown();
        }

        public BitArray Query(object fromkey, object tokey, int maxsize)
        {
            T f = default(T);
            if (typeof(T).Equals(fromkey.GetType()) == false)
                f = (T)Convert.ChangeType(fromkey, typeof(T));
            else
                f = (T)fromkey;

            T t = default(T);
            if (typeof(T).Equals(tokey.GetType()) == false)
                t = (T)Convert.ChangeType(tokey, typeof(T));
            else
                t = (T)tokey;

            return base.Query(f.ToString(), t.ToString(), maxsize);
        }

        object[] IIndex.GetKeys()
        {
            return base.GetKeys();
        }
    }
    #endregion

    #region [  NoIndex  ]
    internal class NoIndex : IIndex
    {
        public void Set(object key, int recnum)
        {
            // ignore set
        }

        public BitArray Query(object fromkey, object tokey, int maxsize)
        {
            throw new NotImplementedException();
        }

        public BitArray Query(RdbExpression ex, object from, int maxsize)
        {
            // always return everything
            return BitArray.Fill(maxsize);
        }

        public void FreeMemory()
        {

        }

        public void Shutdown()
        {

        }

        public void SaveIndex()
        {

        }

        public object[] GetKeys()
        {
            return new object[] { };
        }
    }
    #endregion
}
