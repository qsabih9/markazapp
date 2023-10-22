SELECT *
FROM OPENROWSET (
    BULK 'https://markazapp.dfs.core.windows.net/markazapp-etl/markazapp-transformed/transactions/*.csv',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE
) AS transactions;

SELECT *
FROM OPENROWSET (
    BULK 'https://markazapp.dfs.core.windows.net/markazapp-etl/markazapp-transformed/total_orders_per_category/*.csv',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE
) AS total_orders_per_category ;

SELECT *
FROM OPENROWSET (
    BULK 'https://markazapp.dfs.core.windows.net/markazapp-etl/markazapp-transformed/revenue_per_product/*.csv',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE
) AS revenue_per_product ;

SELECT *
FROM OPENROWSET (
    BULK 'https://markazapp.dfs.core.windows.net/markazapp-etl/markazapp-transformed/rating_stats/*.csv',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE
) AS rating_stats ; 

SELECT *
FROM OPENROWSET (
    BULK 'https://markazapp.dfs.core.windows.net/markazapp-etl/markazapp-transformed/average_ratings/*.csv',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE
) AS average_ratings ;