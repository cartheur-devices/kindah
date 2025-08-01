﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using UhooIndexer.FastJson;

namespace UhooIndexer.FastBinaryJson
{
    internal sealed class BjsonSerializer : IDisposable
    {
        private MemoryStream _output = new MemoryStream();
        private MemoryStream _before = new MemoryStream();
        private readonly int _maxDepth = 20;
        int _currentDepth = 0;
        private readonly Dictionary<string, int> _globalTypes = new Dictionary<string, int>();
        private readonly Dictionary<object, int> _cirobj = new Dictionary<object, int>();
        private readonly BinaryJsonParameters _params;

        private void Dispose(bool disposing)
        {
            if (disposing)
            {
                // dispose managed resources
                _output.Close();
                _before.Close();
            }
            // free native resources
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        internal BjsonSerializer(BinaryJsonParameters param)
        {
            _params = param;
            _maxDepth = param.SerializerMaxDepth;
        }

        internal byte[] ConvertToBjson(object obj)
        {
            WriteValue(obj);

            // add $types
            if (_params.UsingGlobalTypes && _globalTypes != null && _globalTypes.Count > 0)
            {
                byte[] after = _output.ToArray();
                _output = _before;
                WriteName("$types");
                WriteColon();
                WriteTypes(_globalTypes);
                WriteComma();
                _output.Write(after, 0, after.Length);

                return _output.ToArray();
            }

            return _output.ToArray();
        }

        private void WriteTypes(Dictionary<string, int> dic)
        {
            _output.WriteByte(TOKENS.DOC_START);

            bool pendingSeparator = false;

            foreach (var entry in dic)
            {
                if (pendingSeparator) WriteComma();

                WritePair(entry.Value.ToString(), entry.Key);

                pendingSeparator = true;
            }
            _output.WriteByte(TOKENS.DOC_END);
        }

        private void WriteValue(object obj)
        {
            if (obj == null || obj is DBNull)
                WriteNull();

            else if (obj is string)
                WriteString((string)obj);

            else if (obj is char)
                WriteChar((char)obj);

            else if (obj is Guid)
                WriteGuid((Guid)obj);

            else if (obj is bool)
                WriteBool((bool)obj);

            else if (obj is int)
                WriteInt((int)obj);

            else if (obj is uint)
                WriteUInt((uint)obj);

            else if (obj is long)
                WriteLong((long)obj);

            else if (obj is ulong)
                WriteULong((ulong)obj);

            else if (obj is decimal)
                WriteDecimal((decimal)obj);

            else if (obj is byte)
                WriteByte((byte)obj);

            else if (obj is double)
                WriteDouble((double)obj);

            else if (obj is float)
                WriteFloat((float)obj);

            else if (obj is short)
                WriteShort((short)obj);

            else if (obj is ushort)
                WriteUShort((ushort)obj);

            else if (obj is DateTime)
                WriteDateTime((DateTime)obj);

            else if (obj is IDictionary && obj.GetType().IsGenericType && obj.GetType().GetGenericArguments()[0] == typeof(string))
                WriteStringDictionary((IDictionary)obj);

            else if (obj is IDictionary)
                WriteDictionary((IDictionary)obj);

            else if (obj is byte[])
                WriteBytes((byte[])obj);

            else if (obj is StringDictionary)
                WriteSD((StringDictionary)obj);

            else if (obj is NameValueCollection)
                WriteNV((NameValueCollection)obj);

            else if (obj is IEnumerable)
                WriteArray((IEnumerable)obj);

            else if (obj is Enum)
                WriteEnum((Enum)obj);

            else if (Reflection.Instance.IsTypeRegistered(obj.GetType()))
                WriteCustom(obj);

            else
                WriteObject(obj);
        }

        private void WriteNV(NameValueCollection nameValueCollection)
        {
            _output.WriteByte(TOKENS.DOC_START);

            bool pendingSeparator = false;

            foreach (string key in nameValueCollection)
            {
                if (pendingSeparator) _output.WriteByte(TOKENS.COMMA);

                WritePair(key, nameValueCollection[key]);

                pendingSeparator = true;
            }
            _output.WriteByte(TOKENS.DOC_END);
        }

        private void WriteSD(StringDictionary stringDictionary)
        {
            _output.WriteByte(TOKENS.DOC_START);

            bool pendingSeparator = false;

            foreach (DictionaryEntry entry in stringDictionary)
            {
                if (pendingSeparator) _output.WriteByte(TOKENS.COMMA);

                WritePair((string)entry.Key, entry.Value);

                pendingSeparator = true;
            }
            _output.WriteByte(TOKENS.DOC_END);
        }

        private void WriteUShort(ushort p)
        {
            _output.WriteByte(TOKENS.USHORT);
            _output.Write(Helper.GetBytes(p, false), 0, 2);
        }

        private void WriteShort(short p)
        {
            _output.WriteByte(TOKENS.SHORT);
            _output.Write(Helper.GetBytes(p, false), 0, 2);
        }

        private void WriteFloat(float p)
        {
            _output.WriteByte(TOKENS.FLOAT);
            byte[] b = BitConverter.GetBytes(p);
            _output.Write(b, 0, b.Length);
        }

        private void WriteDouble(double p)
        {
            _output.WriteByte(TOKENS.DOUBLE);
            var b = BitConverter.GetBytes(p);
            _output.Write(b, 0, b.Length);
        }

        private void WriteByte(byte p)
        {
            _output.WriteByte(TOKENS.BYTE);
            _output.WriteByte(p);
        }

        private void WriteDecimal(decimal p)
        {
            _output.WriteByte(TOKENS.DECIMAL);
            var b = decimal.GetBits(p);
            foreach (var c in b)
                _output.Write(Helper.GetBytes(c, false), 0, 4);
        }

        private void WriteULong(ulong p)
        {
            _output.WriteByte(TOKENS.ULONG);
            _output.Write(Helper.GetBytes((long)p, false), 0, 8);
        }

        private void WriteUInt(uint p)
        {
            _output.WriteByte(TOKENS.UINT);
            _output.Write(Helper.GetBytes(p, false), 0, 4);
        }

        private void WriteLong(long p)
        {
            _output.WriteByte(TOKENS.LONG);
            _output.Write(Helper.GetBytes(p, false), 0, 8);
        }

        private void WriteChar(char p)
        {
            _output.WriteByte(TOKENS.CHAR);
            _output.Write(Helper.GetBytes((short)p, false), 0, 2);
        }

        private void WriteBytes(byte[] p)
        {
            _output.WriteByte(TOKENS.BYTEARRAY);
            _output.Write(Helper.GetBytes(p.Length, false), 0, 4);
            _output.Write(p, 0, p.Length);
        }

        private void WriteBool(bool p)
        {
            if (p)
                _output.WriteByte(TOKENS.TRUE);
            else
                _output.WriteByte(TOKENS.FALSE);
        }

        private void WriteNull()
        {
            _output.WriteByte(TOKENS.NULL);
        }


        private void WriteCustom(object obj)
        {
            Serialize s;
            Reflection.Instance._customSerializer.TryGetValue(obj.GetType(), out s);
            WriteString(s(obj));
        }

        private void WriteColon()
        {
            _output.WriteByte(TOKENS.COLON);
        }

        private void WriteComma()
        {
            _output.WriteByte(TOKENS.COMMA);
        }

        private void WriteEnum(Enum e)
        {
            WriteString(e.ToString());
        }

        private void WriteInt(int i)
        {
            _output.WriteByte(TOKENS.INT);
            _output.Write(Helper.GetBytes(i, false), 0, 4);
        }

        private void WriteGuid(Guid g)
        {
            _output.WriteByte(TOKENS.GUID);
            _output.Write(g.ToByteArray(), 0, 16);
        }

        private void WriteDateTime(DateTime dateTime)
        {
            DateTime dt = dateTime;
            if (_params.UseUTCDateTime)
                dt = dateTime.ToUniversalTime();

            _output.WriteByte(TOKENS.DATETIME);
            byte[] b = Helper.GetBytes(dt.Ticks, false);
            _output.Write(b, 0, b.Length);
        }

        bool _TypesWritten = false;

        private void WriteObject(object obj)
        {
            int i = 0;
            if (_cirobj.TryGetValue(obj, out i) == false)
                _cirobj.Add(obj, _cirobj.Count + 1);
            else
            {
                if (_currentDepth > 0)
                {
                    //_circular = true;
                    _output.WriteByte(TOKENS.DOC_START);
                    WriteName("$i");
                    WriteColon();
                    WriteValue(i);
                    _output.WriteByte(TOKENS.DOC_END);
                    return;
                }
            }
            if (_params.UsingGlobalTypes == false)
                _output.WriteByte(TOKENS.DOC_START);
            else
            {
                if (_TypesWritten == false)
                {
                    _output.WriteByte(TOKENS.DOC_START);
                    _before = _output;
                    _output = new MemoryStream();
                }
                else
                    _output.WriteByte(TOKENS.DOC_START);

            }
            _TypesWritten = true;
            _currentDepth++;
            if (_currentDepth > _maxDepth)
                throw new Exception("Serializer encountered maximum depth of " + _maxDepth);

            Type t = obj.GetType();
            bool append = false;
            if (_params.UseExtensions)
            {
                if (_params.UsingGlobalTypes == false)
                    WritePairFast("$type", Reflection.Instance.GetTypeAssemblyName(t));
                else
                {
                    int dt = 0;
                    string ct = Reflection.Instance.GetTypeAssemblyName(t);
                    if (_globalTypes.TryGetValue(ct, out dt) == false)
                    {
                        dt = _globalTypes.Count + 1;
                        _globalTypes.Add(ct, dt);
                    }
                    WritePairFast("$type", dt.ToString());
                }
                append = true;
            }

            Getters[] g = Reflection.Instance.GetGetters(t, _params.ShowReadOnlyProperties, _params.IgnoreAttributes);
            int c = g.Length;
            for (int ii = 0; ii < c; ii++)
            {
                var p = g[ii];
                var o = p.Getter(obj);
                if (_params.SerializeNulls == false && (o == null || o is DBNull))
                {
                    
                }
                else
                {
                    if (append)
                        WriteComma();
                    WritePair(p.Name, o);
                    append = true;
                }
            }
            _output.WriteByte(TOKENS.DOC_END);
            _currentDepth--;
        }

        private void WritePairFast(string name, string value)
        {
            if ( _params.SerializeNulls == false && (value == null))
                return;
            WriteName(name);

            WriteColon();

            WriteString(value);
        }

        private void WritePair(string name, object value)
        {
            if (_params.SerializeNulls == false && (value == null || value is DBNull))
                return;
            WriteName(name);

            WriteColon();

            WriteValue(value);
        }

        private void WriteArray(IEnumerable array)
        {
            _output.WriteByte(TOKENS.ARRAY_START);

            bool pendingSeperator = false;

            foreach (object obj in array)
            {
                if (pendingSeperator) WriteComma();

                WriteValue(obj);

                pendingSeperator = true;
            }
            _output.WriteByte(TOKENS.ARRAY_END);
        }

        private void WriteStringDictionary(IDictionary dic)
        {
            _output.WriteByte(TOKENS.DOC_START);

            bool pendingSeparator = false;

            foreach (DictionaryEntry entry in dic)
            {
                if (pendingSeparator) WriteComma();

                WritePair((string)entry.Key, entry.Value);

                pendingSeparator = true;
            }
            _output.WriteByte(TOKENS.DOC_END);
        }

        private void WriteStringDictionary(IDictionary<string, object> dic)
        {
            _output.WriteByte(TOKENS.DOC_START);

            bool pendingSeparator = false;

            foreach (KeyValuePair<string, object> entry in dic)
            {
                if (pendingSeparator) WriteComma();

                WritePair((string)entry.Key, entry.Value);

                pendingSeparator = true;
            }
            _output.WriteByte(TOKENS.DOC_END);
        }

        private void WriteDictionary(IDictionary dic)
        {
            _output.WriteByte(TOKENS.ARRAY_START);

            bool pendingSeparator = false;

            foreach (DictionaryEntry entry in dic)
            {
                if (pendingSeparator) WriteComma();
                _output.WriteByte(TOKENS.DOC_START);
                WritePair("k", entry.Key);
                WriteComma();
                WritePair("v", entry.Value);
                _output.WriteByte(TOKENS.DOC_END);

                pendingSeparator = true;
            }
            _output.WriteByte(TOKENS.ARRAY_END);
        }

        private void WriteName(string s)
        {
            _output.WriteByte(TOKENS.NAME);
            byte[] b = Reflection.Instance.utf8.GetBytes(s);
            _output.WriteByte((byte)b.Length);
            _output.Write(b, 0, b.Length % 256);
        }

        private void WriteString(string s)
        {
            byte[] b = null;
            if (_params.UseUnicodeStrings)
            {
                _output.WriteByte(TOKENS.UNICODE_STRING);
                b = Reflection.Instance.unicode.GetBytes(s);
            }
            else
            {
                _output.WriteByte(TOKENS.STRING);
                b = Reflection.Instance.utf8.GetBytes(s);
            }
            _output.Write(Helper.GetBytes(b.Length, false), 0, 4);
            _output.Write(b, 0, b.Length);
        }
    }
}
