﻿using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;
using UhooIndexer.MgIndex;

namespace UhooIndexer
{
    internal class SafeDictionary<TKey, TValue>
    {
        private readonly object _padlock = new object();
        private readonly Dictionary<TKey, TValue> _dictionary;

        public SafeDictionary(int capacity)
        {
            _dictionary = new Dictionary<TKey, TValue>(capacity);
        }

        public SafeDictionary()
        {
            _dictionary = new Dictionary<TKey, TValue>();
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            lock (_padlock)
                return _dictionary.TryGetValue(key, out value);
        }

        public TValue this[TKey key]
        {
            get
            {
                lock (_padlock)
                    return _dictionary[key];
            }
            set
            {
                lock (_padlock)
                    _dictionary[key] = value;
            }
        }

        public int Count
        {
            get { lock (_padlock) return _dictionary.Count; }
        }

        public ICollection<KeyValuePair<TKey, TValue>> GetList()
        {
            return _dictionary;
        }

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            return ((ICollection<KeyValuePair<TKey, TValue>>)_dictionary).GetEnumerator();
        }

        public void Add(TKey key, TValue value)
        {
            lock (_padlock)
            {
                if (_dictionary.ContainsKey(key) == false)
                    _dictionary.Add(key, value);
            }
        }

        public TKey[] Keys()
        {
            lock (_padlock)
            {
                TKey[] keys = new TKey[_dictionary.Keys.Count];
                _dictionary.Keys.CopyTo(keys, 0);
                return keys;
            }
        }

        public bool Remove(TKey key)
        {
            lock (_padlock)
                return _dictionary.Remove(key);
        }
    }

    public class SafeSortedList<T, V>
    {
        private readonly object _padlock = new object();
        readonly SortedList<T, V> _list = new SortedList<T, V>();

        public int Count
        {
            get { lock (_padlock) return _list.Count; }
        }

        public void Add(T key, V val)
        {
            lock (_padlock)
                if (_list.ContainsKey(key) == false)
                    _list.Add(key, val);
                else
                    _list[key] = val;
        }

        public void Remove(T key)
        {
            if (key == null)
                return;
            lock (_padlock)
                _list.Remove(key);
        }

        public T GetKey(int index)
        {
            lock (_padlock) return _list.Keys[index];
        }

        public V GetValue(int index)
        {
            lock (_padlock) return _list.Values[index];
        }

        public T[] Keys()
        {
            lock (_padlock)
            {
                T[] keys = new T[_list.Keys.Count];
                _list.Keys.CopyTo(keys, 0);
                return keys;
            }
        }

        public IEnumerator<KeyValuePair<T, V>> GetEnumerator()
        {
            return ((ICollection<KeyValuePair<T, V>>)_list).GetEnumerator();
        }

        public bool TryGetValue(T key, out V value)
        {
            lock (_padlock)
                return _list.TryGetValue(key, out value);
        }

        public V this[T key]
        {
            get
            {
                lock (_padlock)
                    return _list[key];
            }
            set
            {
                lock (_padlock)
                    _list[key] = value;
            }
        }
    }

    //------------------------------------------------------------------------------------------------------------------

    internal static class FastDateTime
    {
        public static TimeSpan LocalUtcOffset;

        public static DateTime Now
        {
            get { return DateTime.UtcNow + LocalUtcOffset; }
        }

        static FastDateTime()
        {
            LocalUtcOffset = TimeZone.CurrentTimeZone.GetUtcOffset(DateTime.Now);
        }
    }

    //------------------------------------------------------------------------------------------------------------------

    internal static class Helper
    {
        public static MurmurHash2Unsafe MurMur = new MurmurHash2Unsafe();
        public static int CompareMemCmp(byte[] left, byte[] right)
        {
            int c = left.Length;
            if (c > right.Length)
                c = right.Length;
            return memcmp(left, right, c);
        }

        [DllImport("msvcrt.dll", CallingConvention = CallingConvention.Cdecl)]
        private static extern int memcmp(byte[] arr1, byte[] arr2, int cnt);

        internal static unsafe int ToInt32(byte[] value, int startIndex, bool reverse)
        {
            if (reverse)
            {
                byte[] b = new byte[4];
                Buffer.BlockCopy(value, startIndex, b, 0, 4);
                Array.Reverse(b);
                return ToInt32(b, 0);
            }

            return ToInt32(value, startIndex);
        }

        internal static unsafe int ToInt32(byte[] value, int startIndex)
        {
            fixed (byte* numRef = &(value[startIndex]))
            {
                return *((int*)numRef);
            }
        }

        internal static unsafe long ToInt64(byte[] value, int startIndex, bool reverse)
        {
            if (reverse)
            {
                byte[] b = new byte[8];
                Buffer.BlockCopy(value, startIndex, b, 0, 8);
                Array.Reverse(b);
                return ToInt64(b, 0);
            }
            return ToInt64(value, startIndex);
        }

        internal static unsafe long ToInt64(byte[] value, int startIndex)
        {
            fixed (byte* numRef = &(value[startIndex]))
            {
                return *(((long*)numRef));
            }
        }

        internal static unsafe short ToInt16(byte[] value, int startIndex, bool reverse)
        {
            if (reverse)
            {
                byte[] b = new byte[2];
                Buffer.BlockCopy(value, startIndex, b, 0, 2);
                Array.Reverse(b);
                return ToInt16(b, 0);
            }
            return ToInt16(value, startIndex);
        }

        internal static unsafe short ToInt16(byte[] value, int startIndex)
        {
            fixed (byte* numRef = &(value[startIndex]))
            {
                return *(((short*)numRef));
            }
        }

        internal static unsafe byte[] GetBytes(long num, bool reverse)
        {
            byte[] buffer = new byte[8];
            fixed (byte* numRef = buffer)
            {
                *((long*)numRef) = num;
            }
            if (reverse)
                Array.Reverse(buffer);
            return buffer;
        }

        public static unsafe byte[] GetBytes(int num, bool reverse)
        {
            byte[] buffer = new byte[4];
            fixed (byte* numRef = buffer)
            {
                *((int*)numRef) = num;
            }
            if (reverse)
                Array.Reverse(buffer);
            return buffer;
        }

        public static unsafe byte[] GetBytes(short num, bool reverse)
        {
            byte[] buffer = new byte[2];
            fixed (byte* numRef = buffer)
            {
                *((short*)numRef) = num;
            }
            if (reverse)
                Array.Reverse(buffer);
            return buffer;
        }

        public static byte[] GetBytes(string s)
        {
            return Encoding.UTF8.GetBytes(s);
        }

        internal static string GetString(byte[] buffer, int index, short keylength)
        {
            return Encoding.UTF8.GetString(buffer, index, keylength);
        }
    }
}
