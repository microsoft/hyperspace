class IncludedColumnsConfig:
    def __init__(self, include, exclude):
        """
        Initializes IncludedColumnsConfig object.
        :param include: List of column names to include as include columns.
        :param exclude: List of column names to exclude to form list of included columns.
        :return: IncludedColumnsConfig object.

        >>> ixIncludedColumns = IncludedColumnsConfig(["c1"], ["c2","c3"])
        """
        self.include = include
        self.exclude = exclude