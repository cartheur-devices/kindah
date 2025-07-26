//
// This intelligent search software is the property of Cartheur Robotics, spol. s r.o. Copyright 2019, all rights reserved.
//
using System;
using System.IO;
using System.Text;

namespace UhooIndexer.Utilities
{
    /// <summary>
    /// The class which performs logging for the library. Originated in MacOS 9.0.4 (via CodeWarrior in SheepShaver, September - December 2014).
    /// </summary>
    public static class Logging
    {
        private static string LogModelName { get { return @"logfile"; } }
        private static string TranscriptModelName { get { return @"transcript"; } }
        /// <summary>
        /// The type of model to use for logging.
        /// </summary>
        public static string LogModelFile { get; set; }
        /// <summary>
        /// The type of model to use for the transcript.
        /// </summary>
        public static string TranscriptModelFile { get; set; }
        /// <summary>
        /// The file path for executing assemblies.
        /// </summary>
        public static string FilePath()
        {
            return Environment.CurrentDirectory;
        }
        /// <summary>
        /// The type of log to write.
        /// </summary>
        public enum LogType
        {
            /// <summary>
            /// The informational or debugging log.
            /// </summary>
            Information,
            /// <summary>
            /// The error log.
            /// </summary>
            Error,
            /// <summary>
            /// The warning log.
            /// </summary>
            Warning
        };
        /// <summary>
        /// The classes within the interpreter calling the log.
        /// </summary>
        public enum LogCaller
        {
            BitMapIndex,
            IndexFile,
            KeyStore,
            KeyStoreHf,
            /// <summary>
            /// The kindah application.
            /// </summary>
            KindahApplication,
            /// <summary>
            /// M.E.
            /// </summary>
            Me,
            MgIndex,
            /// <summary>
            /// The shared function.
            /// </summary>
            SharedFunction,
            StorageFile,
            /// <summary>
            /// The test framework.
            /// </summary>
            TestFramework,
            Uhoo
        }
        /// <summary>
        /// The last message passed to logging.
        /// </summary>
        public static string LastMessage = "";
        /// <summary>
        /// The delegate for returning the last log message to the calling application.
        /// </summary>
        public delegate void LoggingDelegate();
        /// <summary>
        /// Occurs when [returned to console] is called.
        /// </summary>
        public static event LoggingDelegate ReturnedToConsole;
        /// <summary>
        /// Optional means to model the logfile from its original "logfile" model.
        /// </summary>
        /// <param name="modelName"></param>
        /// <returns>The path for the logfile.</returns>
        public static void ChangeLogModel(string modelName)
        {
            LogModelFile = modelName;
        }
        /// <summary>
        /// Logs a message sent from the calling application to a file.
        /// </summary>
        /// <param name="message">The message to log. Space between the message and log type enumeration provided.</param>
        /// <param name="logType">Type of the log.</param>
        /// <param name="caller">The class creating the log entry.</param>
        public static void WriteLog(string message, LogType logType, LogCaller caller)
        {
            if(LogModelFile == null)
            {
                LogModelFile = LogModelName;
            }
            LastMessage = message;
			// Use FilePath() when outside of a test framework.
            var stream = new StreamWriter(FilePath() + @"\logs\" + LogModelFile + @".txt", true);
            switch (logType)
            {
                case LogType.Error:
                    stream.WriteLine(DateTime.Now + " - " + " ERROR " + " - " + message + " from class " + caller + ".");
                    break;
                case LogType.Warning:
                    stream.WriteLine(DateTime.Now + " - " + " WARNING " + " - " + message + " from class " + caller + ".");
                    break;
                case LogType.Information:
                    stream.WriteLine(DateTime.Now + " - " + message + " This was called from the class " + caller + ".");
                    break;
            }
            stream.Close();
            if (!Equals(null, ReturnedToConsole))
            {
                ReturnedToConsole();
            }
        }
        /// <summary>
        /// Records a transcript of the conversation.
        /// </summary>
        /// <param name="message">The message to save in transcript format.</param>
        public static void RecordTranscript(string message)
        {
            if (TranscriptModelFile == "")
            {
                TranscriptModelFile = TranscriptModelName;
            }
            try
            {
				// Use FilePath() when outside of a test framework.
                StreamWriter stream = new StreamWriter(FilePath() + @"\logs\" + TranscriptModelFile + @".txt", true);
                stream.WriteLine(DateTime.Now + " - " + message);
                stream.Close();
            }
            catch (Exception ex)
            {
                WriteLog(ex.Message, LogType.Error, LogCaller.Me);
            }
            
        }
        /// <summary>
        /// Saves the last result to support analysis of the algorithm.
        /// </summary>
        /// <param name="output">The output from the conversation.</param>
        public static void SaveLastResult(StringBuilder output)
        {
            try
            {
                // Use FilePath() when outside of a test framework.
                StreamWriter stream = new StreamWriter(FilePath() + @"\db\analytics.txt", true);
                stream.WriteLine(DateTime.Now + " - " + output);
                stream.Close();
            }
            catch (Exception ex)
            {
                WriteLog(ex.Message, LogType.Error, LogCaller.Me);
            }

        }
        /// <summary>
        /// Saves the last result to support analysis of the algorithm to storage.
        /// </summary>
        /// <param name="output">The output from the conversation.</param>
        public static void SaveLastResultToStorage(StringBuilder output)
        {
            try
            {
                // Use FilePath() when outside of a test framework.
                StreamWriter stream = new StreamWriter(FilePath() + @"\db\analyticsStorage.txt", true);
                stream.WriteLine("#" + DateTime.Now + ";" + output);
                stream.Close();
            }
            catch (Exception ex)
            {
                WriteLog(ex.Message, LogType.Error, LogCaller.Me);
            }

        }
        /// <summary>
        /// Writes a debug log with object parameters.
        /// </summary>
        /// <param name="objects">The objects.</param>
        public static void Debug(params object[] objects)
        {
            // Use FilePath() when outside of a test framework.
            StreamWriter stream = new StreamWriter(FilePath() + @"\logs\debugdump.txt", true);
            foreach (object obj in objects)
            {
                stream.WriteLine(obj);
            }
            stream.WriteLine("--");
            stream.Close();
        }
    }
}
