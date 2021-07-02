class CoveringIndexConfig:
    def __init__(self, indexName, indexedColumns, includedColumns):
        """
        Initializes CoveringIndexConfig object.
        :param indexName: index name
        :param indexedColumns: indexed columns
        :param includedColumns: included columns
        :return: CoveringIndexConfig object

        >>> idxConfig = CoveringIndexConfig("indexName", ["c1"], ["c2","c3"])
        """
        self.indexName = indexName
        self.indexedColumns = indexedColumns
        self.includedColumns = includedColumns

IndexConfig = CoveringIndexConfig
