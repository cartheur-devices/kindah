namespace UhooIndexer.MgIndex
{
    internal interface IIndex
    {
        void Set(object key, int recnum);
        BitArray Query(object fromkey, object tokey, int maxsize);
        BitArray Query(RdbExpression ex, object from, int maxsize);
        void FreeMemory();
        void Shutdown();
        void SaveIndex();
        object[] GetKeys();
    }
}