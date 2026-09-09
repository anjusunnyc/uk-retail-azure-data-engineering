-- Synapse Serverless SQL security example
-- Replace the placeholder with the Microsoft Entra identity used in your environment.
-- Personal tenant/user identifiers are intentionally not committed to the public repository.

CREATE USER [<azure-user>@<tenant>.onmicrosoft.com]
FROM EXTERNAL PROVIDER;
GO

ALTER ROLE db_datareader
ADD MEMBER [<azure-user>@<tenant>.onmicrosoft.com];
GO

GRANT ADMINISTER DATABASE BULK OPERATIONS
TO [<azure-user>@<tenant>.onmicrosoft.com];
GO
