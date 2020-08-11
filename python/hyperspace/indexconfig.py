class IndexConfig:
    def __init__(self, indexName, indexedColumns, includedColumns):
        """
        Initializes IndexConfig object.
        :param indexName: index name
        :param indexedColumns: indexed columns
        :param includedColumns: included columns
        :return: IndexConfig object

        >>> idxConfig = IndexConfig("indexName", ["c1"], ["c2","c3"])
        """
        self.indexName = indexName
        self.indexedColumns = indexedColumns
        self.includedColumns = includedColumns
