using System;
using System.Collections.Generic;
using UhooIndexer.FastJson;

namespace UhooIndexer.FastBinaryJson
{
    public sealed class TOKENS
    {
        public const byte DOC_START = 1;
        public const byte DOC_END = 2;
        public const byte ARRAY_START = 3;
        public const byte ARRAY_END = 4;
        public const byte COLON = 5;
        public const byte COMMA = 6;
        public const byte NAME = 7;
        public const byte STRING = 8;
        public const byte BYTE = 9;
        public const byte INT = 10;
        public const byte UINT = 11;
        public const byte LONG = 12;
        public const byte ULONG = 13;
        public const byte SHORT = 14;
        public const byte USHORT = 15;
        public const byte DATETIME = 16;
        public const byte GUID = 17;
        public const byte DOUBLE = 18;
        public const byte FLOAT = 19;
        public const byte DECIMAL = 20;
        public const byte CHAR = 21;
        public const byte BYTEARRAY = 22;
        public const byte NULL = 23;
        public const byte TRUE = 24;
        public const byte FALSE = 25;
        public const byte UNICODE_STRING = 26;
        public const byte DATETIMEOFFSET = 27;
    }

    //public delegate string Serialize(object data);
    //public delegate object Deserialize(string data);

    public static class BinaryJson
    {
        /// <summary>
        /// Globally set-able parameters for controlling the serializer
        /// </summary>
        public static BinaryJsonParameters Parameters = new BinaryJsonParameters();
        /// <summary>
        /// Parse a json and generate a Dictionary&lt;string,object&gt; or List&lt;object&gt; structure
        /// </summary>
        /// <param name="json"></param>
        /// <returns></returns>
        public static object Parse(byte[] json)
        {
            return new BinaryJsonParser(json, Parameters.UseUTCDateTime).Decode();
        }
        /// <summary>
        /// Register custom type handlers for your own types not natively handled by fastBinaryJSON
        /// </summary>
        /// <param name="type"></param>
        /// <param name="serializer"></param>
        /// <param name="deserializer"></param>
        public static void RegisterCustomType(Type type, Serialize serializer, Deserialize deserializer)
        {
            Reflection.Instance.RegisterCustomType(type, serializer, deserializer);
        }
        /// <summary>
        /// Create a binary json representation for an object
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public static byte[] ToBjson(object obj)
        {
            return ToBjson(obj, Parameters);
        }
        /// <summary>
        /// Create a binary json representation for an object with parameter override on this call
        /// </summary>
        /// <param name="obj"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public static byte[] ToBjson(object obj, BinaryJsonParameters param)
        {
            param.FixValues();
            Type t = null;
            if (obj == null)
                return new byte[] { TOKENS.NULL };
            if (obj.GetType().IsGenericType)
                t = Reflection.Instance.GetGenericTypeDefinition(obj.GetType());// obj.GetType().GetGenericTypeDefinition();
            if (t == typeof(Dictionary<,>) || t == typeof(List<>))
                param.UsingGlobalTypes = false;
            // FEATURE : enable extensions when you can deserialize anon types
            if (param.EnableAnonymousTypes) { param.UseExtensions = false; param.UsingGlobalTypes = false; }

            return new BjsonSerializer(param).ConvertToBjson(obj);
        }
        /// <summary>
        /// Fill a given object with the binary json represenation
        /// </summary>
        /// <param name="input"></param>
        /// <param name="json"></param>
        /// <returns></returns>
        public static object FillObject(object input, byte[] json)
        {
            return new Deserializer(Parameters).FillObject(input, json);
        }
        /// <summary>
        /// Create a generic object from the json
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="json"></param>
        /// <returns></returns>
        public static T ToObject<T>(byte[] json)
        {
            return new Deserializer(Parameters).ToObject<T>(json);
        }
        /// <summary>
        /// Create a generic object from the json with parameter override on this call
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="json"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public static T ToObject<T>(byte[] json, BinaryJsonParameters param)
        {
            return new Deserializer(param).ToObject<T>(json);
        }
        /// <summary>
        /// Create an object from the json 
        /// </summary>
        /// <param name="json"></param>
        /// <returns></returns>
        public static object ToObject(byte[] json)
        {
            return new Deserializer(Parameters).ToObject(json, null);
        }
        /// <summary>
        /// Create an object from the json with parameter override on this call
        /// </summary>
        /// <param name="json"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public static object ToObject(byte[] json, BinaryJsonParameters param)
        {
            param.FixValues();
            return new Deserializer(param).ToObject(json, null);
        }
        /// <summary>
        /// Create a typed object from the json
        /// </summary>
        /// <param name="json"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public static object ToObject(byte[] json, Type type)
        {
            return new Deserializer(Parameters).ToObject(json, type);
        }
        /// <summary>
        /// Clear the internal reflection cache so you can start from new (you will loose performance)
        /// </summary>
        public static void ClearReflectionCache()
        {
            Reflection.Instance.ClearReflectionCache();
        }
        /// <summary>
        /// Deep copy an object i.e. clone to a new object
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public static object DeepCopy(object obj)
        {
            return new Deserializer(Parameters).ToObject(ToBjson(obj));
        }
    }
}