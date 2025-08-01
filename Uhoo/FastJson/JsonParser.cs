using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace UhooIndexer.FastJson
{
    /// <summary>
    /// This class encodes and decodes JSON strings.
    /// </summary>
    internal sealed class JsonParser
    {
        /// <summary>
        /// The json token.
        /// </summary>
        /// <remarks>Used to denote no Lookahead available</remarks>
        enum Token
        {
            None = -1,        
            CurlyOpen,
            CurlyClose,
            SquaredOpen,
            SquaredClose,
            Colon,
            Comma,
            String,
            Number,
            True,
            False,
            Null
        }

        readonly string json;
        readonly StringBuilder s = new StringBuilder(); // used for inner string parsing " \"\r\n\u1234\'\t " 
        Token lookAheadToken = Token.None;
        int index;

        internal JsonParser(string json)
        {
            this.json = json;
        }

        public object Decode()
        {
            return ParseValue();
        }

        private Dictionary<string, object> ParseObject()
        {
            Dictionary<string, object> table = new Dictionary<string, object>();

            ConsumeToken(); // {

            while (true)
            {
                switch (LookAhead())
                {

                    case Token.Comma:
                        ConsumeToken();
                        break;

                    case Token.CurlyClose:
                        ConsumeToken();
                        return table;

                    default:
                        {
                            // name
                            string name = ParseString();

                            // :
                            if (NextToken() != Token.Colon)
                            {
                                throw new Exception("Expected colon at index " + index);
                            }

                            // value
                            object value = ParseValue();

                            table[name] = value;
                        }
                        break;
                }
            }
        }

        private List<object> ParseArray()
        {
            List<object> array = new List<object>();
            ConsumeToken(); // [

            while (true)
            {
                switch (LookAhead())
                {
                    case Token.Comma:
                        ConsumeToken();
                        break;

                    case Token.SquaredClose:
                        ConsumeToken();
                        return array;

                    default:
                        array.Add(ParseValue());
                        break;
                }
            }
        }

        private object ParseValue()
        {
            switch (LookAhead())
            {
                case Token.Number:
                    return ParseNumber();

                case Token.String:
                    return ParseString();

                case Token.CurlyOpen:
                    return ParseObject();

                case Token.SquaredOpen:
                    return ParseArray();

                case Token.True:
                    ConsumeToken();
                    return true;

                case Token.False:
                    ConsumeToken();
                    return false;

                case Token.Null:
                    ConsumeToken();
                    return null;
            }

            throw new Exception("Unrecognized token at index" + index);
        }

        private string ParseString()
        {
            ConsumeToken(); // "

            s.Length = 0;

            int runIndex = -1;
            int l = json.Length;
            //fixed (char* p = json)
            string p = json;
            {
                while (index < l)
                {
                    var c = p[index++];

                    if (c == '"')
                    {
                        if (runIndex != -1)
                        {
                            if (s.Length == 0)
                                return json.Substring(runIndex, index - runIndex - 1);

                            s.Append(json, runIndex, index - runIndex - 1);
                        }
                        return s.ToString();
                    }

                    if (c != '\\')
                    {
                        if (runIndex == -1)
                            runIndex = index - 1;

                        continue;
                    }

                    if (index == l) break;

                    if (runIndex != -1)
                    {
                        s.Append(json, runIndex, index - runIndex - 1);
                        runIndex = -1;
                    }

                    switch (p[index++])
                    {
                        case '"':
                            s.Append('"');
                            break;

                        case '\\':
                            s.Append('\\');
                            break;

                        case '/':
                            s.Append('/');
                            break;

                        case 'b':
                            s.Append('\b');
                            break;

                        case 'f':
                            s.Append('\f');
                            break;

                        case 'n':
                            s.Append('\n');
                            break;

                        case 'r':
                            s.Append('\r');
                            break;

                        case 't':
                            s.Append('\t');
                            break;

                        case 'u':
                            {
                                int remainingLength = l - index;
                                if (remainingLength < 4) break;

                                // parse the 32 bit hex into an integer codepoint
                                uint codePoint = ParseUnicode(p[index], p[index + 1], p[index + 2], p[index + 3]);
                                s.Append((char)codePoint);

                                // skip 4 chars
                                index += 4;
                            }
                            break;
                    }
                }
            }

            throw new Exception("Unexpectedly reached end of string");
        }

        private uint ParseSingleChar(char c1, uint multipliyer)
        {
            uint p1 = 0;
            if (c1 >= '0' && c1 <= '9')
                p1 = (uint)(c1 - '0') * multipliyer;
            else if (c1 >= 'A' && c1 <= 'F')
                p1 = (uint)((c1 - 'A') + 10) * multipliyer;
            else if (c1 >= 'a' && c1 <= 'f')
                p1 = (uint)((c1 - 'a') + 10) * multipliyer;
            return p1;
        }

        private uint ParseUnicode(char c1, char c2, char c3, char c4)
        {
            uint p1 = ParseSingleChar(c1, 0x1000);
            uint p2 = ParseSingleChar(c2, 0x100);
            uint p3 = ParseSingleChar(c3, 0x10);
            uint p4 = ParseSingleChar(c4, 1);

            return p1 + p2 + p3 + p4;
        }

        private long CreateLong(string s)
        {
            long num = 0;
            bool neg = false;
            foreach (char cc in s)
            {
                if (cc == '-')
                    neg = true;
                else if (cc == '+')
                    neg = false;
                else
                {
                    num *= 10;
                    num += (int)(cc - '0');
                }
            }

            return neg ? -num : num;
        }

        private object ParseNumber()
        {
            ConsumeToken();

            // Need to start back one place because the first digit is also a token and would have been consumed
            var startIndex = index - 1;
            bool dec = false;
            do
            {
                if (index == json.Length)
                    break;
                var c = json[index];

                if ((c >= '0' && c <= '9') || c == '.' || c == '-' || c == '+' || c == 'e' || c == 'E')
                {
                    if (c == '.' || c == 'e' || c == 'E')
                        dec = true;
                    if (++index == json.Length)
                        break;//throw new Exception("Unexpected end of string whilst parsing number");
                    continue;
                }
                break;
            } while (true);

            if (dec)
            {
                string s = json.Substring(startIndex, index - startIndex);
                return double.Parse(s, NumberFormatInfo.InvariantInfo);
            }
            return Json.CreateLong(json, startIndex, index - startIndex);
        }

        private Token LookAhead()
        {
            if (lookAheadToken != Token.None) return lookAheadToken;

            return lookAheadToken = NextTokenCore();
        }

        private void ConsumeToken()
        {
            lookAheadToken = Token.None;
        }

        private Token NextToken()
        {
            var result = lookAheadToken != Token.None ? lookAheadToken : NextTokenCore();

            lookAheadToken = Token.None;

            return result;
        }

        private Token NextTokenCore()
        {
            char c;

            // Skip past whitespace
            do
            {
                c = json[index];

                if (c == '/' && json[index + 1] == '/') // c++ style single line comments
                {
                    index++;
                    index++;
                    do
                    {
                        c = json[index];
                        if (c == '\r' || c == '\n') break; // read till end of line
                    }
                    while (++index < json.Length);
                }
                if (c > ' ') break;
                if (c != ' ' && c != '\t' && c != '\n' && c != '\r') break;

            } while (++index < json.Length);

            if (index == json.Length)
            {
                throw new Exception("Reached end of string unexpectedly");
            }

            c = json[index];

            index++;

            switch (c)
            {
                case '{':
                    return Token.CurlyOpen;

                case '}':
                    return Token.CurlyClose;

                case '[':
                    return Token.SquaredOpen;

                case ']':
                    return Token.SquaredClose;

                case ',':
                    return Token.Comma;

                case '"':
                    return Token.String;

                case '0':
                case '1':
                case '2':
                case '3':
                case '4':
                case '5':
                case '6':
                case '7':
                case '8':
                case '9':
                case '-':
                case '+':
                case '.':
                    return Token.Number;

                case ':':
                    return Token.Colon;

                case 'f':
                    if (json.Length - index >= 4 &&
                        json[index + 0] == 'a' &&
                        json[index + 1] == 'l' &&
                        json[index + 2] == 's' &&
                        json[index + 3] == 'e')
                    {
                        index += 4;
                        return Token.False;
                    }
                    break;

                case 't':
                    if (json.Length - index >= 3 &&
                        json[index + 0] == 'r' &&
                        json[index + 1] == 'u' &&
                        json[index + 2] == 'e')
                    {
                        index += 3;
                        return Token.True;
                    }
                    break;

                case 'n':
                    if (json.Length - index >= 3 &&
                        json[index + 0] == 'u' &&
                        json[index + 1] == 'l' &&
                        json[index + 2] == 'l')
                    {
                        index += 3;
                        return Token.Null;
                    }
                    break;
            }
            throw new Exception("Could not find token at index " + --index);
        }
    }
}
