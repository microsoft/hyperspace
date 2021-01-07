class IndexConfig:
    def __init__(self, indexName, indexedColumns, includedColumns):
        """
        Initializes IndexConfig object.
        :param indexName: index name.
        :param indexedColumns: indexed columns.
        :param includedColumns: IncludedColumns instance.
        :return: IndexConfig object.

        >>> idxConfig = IndexConfig("indexName", ["c1"], IncludedColumns(["c2, c3"], []))
        """
        self.indexName = indexName
        self.indexedColumns = indexedColumns
        self.includedColumns = includedColumns
