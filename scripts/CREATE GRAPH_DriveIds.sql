SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dm].[GRAPH_DriveIds]') AND type in (N'U'))
DROP TABLE [dm].[GRAPH_DriveIds];
GO
CREATE TABLE [dm].[GRAPH_DriveIds]
(
    [ID] [int] IDENTITY(1,1) NOT NULL,
    ReportDateTime [datetime] NOT NULL,
    DriveIdCreatedDateTime [datetime] NULL,
    Description [varchar](250) NULL,
    DriveId [char](66) NOT NULL,
    LastModifiedDateTime [datetime] NULL,
    Name [varchar](100) NOT NULL,
    WebUrl [varchar](155) NULL,
    DriveType [varchar](100) NULL,
    CreatedByUserEmail [varchar](100) NULL,
    CreatedByUserId [char](36) NULL,
    CreatedByUserDisplayName [varchar](100) NULL,
    LastModifiedByUserDisplayName [varchar](100) NULL,
    OwnerUserEmail [varchar](100) NULL,
    OwnerUserId [char](36) NULL,
    OwnerUserDisplayName [varchar](100) NULL,
    LastModifiedByUserEmail [varchar](100) NULL,
    LastModifiedByUserId [char](36) NULL,
    SiteId [char](114) NOT NULL,
    SiteName [varchar](100) NULL,
    QuotaDeleted [bigint] NULL,
    QuotaRemaining [bigint] NULL,
    QuotaState [varchar](20) NULL,
    QuotaTotal [bigint] NULL,
    QuotaUsed [bigint] NULL,
    OwnerGroupId [char](36) NULL,
    OwnerGroupDisplayName [varchar](100) NULL,
    OwnerGroupEmail [varchar](100) NULL
)
WITH
(
    DISTRIBUTION = REPLICATE,
    CLUSTERED INDEX ([Id])
)
GO