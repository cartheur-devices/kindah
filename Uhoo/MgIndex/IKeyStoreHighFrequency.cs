namespace UhooIndexer.MgIndex
{
    /// <summary>
    /// High frequency mode Key/Value store with recycled storage file.
    /// <para>Use for rapid saves of the same key.</para>
    /// <para>Views are not effected by saves in this storage.</para>
    /// <para>NOTE : You do not have history of changes in this storage.</para>
    /// </summary>
    public interface IKeyStoreHighFrequency
    {
        object GetObjectHf(string key);
        bool SetObjectHf(string key, object obj);
        bool DeleteKeyHf(string key);
        int CountHf();
        bool ContainsHf(string key);
        string[] GetKeysHf();
        void CompactStorageHf();
        int Increment(string key, int amount);
        decimal Increment(string key, decimal amount);
        int Decrement(string key, int amount);
        decimal Decrement(string key, decimal amount);
        //T Increment<T>(string key, T amount);
        //T Decrement<T>(string key, T amount);
        //IEnumerable<object> EnumerateObjects();
        //string[] SearchKeys(string contains); // FIX : implement 
    }
}