SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dm].[GRAPH_SiteIds]') AND type in (N'U'))
DROP TABLE [dm].[GRAPH_SiteIds];
GO
CREATE TABLE [dm].[GRAPH_SiteIds]
(
    [ID] [int] IDENTITY(1,1) NOT NULL,
    ReportDateTime [datetime] NOT NULL,
    SiteIdCreatedDateTime [datetime] NULL,
    Description [varchar](250) NULL,
    SiteId [char](114) NOT NULL,
    LastModifiedDateTime [datetime] NULL,
    Name [varchar](100) NOT NULL,
    WebUrl [varchar](155) NULL,
    DisplayName [varchar](100) NOT NULL,
    SiteCollectionHostname [varchar](100) NULL
)
WITH
(
    DISTRIBUTION = REPLICATE,
    CLUSTERED INDEX ([Id])
)
GO