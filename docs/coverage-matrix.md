# Control-M job-type coverage matrix

The exhaustive list of Control-M job types (web-verified against BMC documentation,
2026-07-09) organized by **migration disposition**. Supersedes the 13-row roadmap in
`docs/job-mapping-catalog.md` §6 as the planning reference; the catalog remains the
authoritative record of what the registry implements *today*.

Confidence markers:
- ✓ literal `Job:*` type string confirmed from a BMC JSON example or BMC's ctm-python-client docs
- ○ integration verified on a BMC doc page, exact literal type string not independently confirmed
  (BMC is inconsistent: e.g. `Job:AWS Batch` current vs `Job:AWS:Batch` deprecated, both attested)
- △ secondary source only (community/legacy reference)

**Parser caveat:** this list is the Automation-API JSON `Job:*` enum. Classic XML exports
(what this pipeline parses) carry the older `TASKTYPE`/`APPL_TYPE` attribute dialect for the
same universe; the pre-AAPI enum was not fully retrievable — **real exports remain the ground
truth for spellings** (P0). Total: **~187 types / 24 families** (~95 ✓, ~92 ○).

Key sources: [Integrations catalog](https://documents.bmc.com/supportu/controlm-saas/en-US/Documentation/Integrations_Main.htm) ·
[Data Processing types](https://documents.bmc.com/supportu/controlm-saas/en-US/Documentation/API_CodeRef_JobTypes_DataProcessing.htm) ·
[Cloud Compute types](https://documents.bmc.com/supportu/controlm-saas/en-US/Documentation/API_CodeRef_JobTypes_CloudCompute.htm) ·
[Other types](https://documents.bmc.com/supportu/controlm-saas/en-US/Documentation/API_CodeRef_JobTypes_other.htm) ·
[Agent utilities](https://documents.bmc.com/supportu/controlm-saas/en-US/Documentation/Agent_Utilities.htm) ·
[Server utilities](https://documents.bmc.com/supportu/9.0.21/en-US/Documentation/Server_Utils.htm)

---

## Class 1 — RULES, implemented today (registry rows exist)

| Type | Airflow mapping | Status |
|---|---|---|
| ✓ Job:Command / classic Command | SSHOperator / WinRMOperator (node os + PS sniff) | FULL |
| ✓ Job:Script / classic Job (MEMLIB/MEMNAME) | SSHOperator / WinRMOperator | FULL |
| ✓ Job:EmbeddedScript | SSH/WinRM (inline script) — parser treats as command | FULL |
| ✓ Job:Dummy / classic Dummy | EmptyOperator | FULL |
| ✓ Job:FileWatcher:Create / :Delete / classic FILEWATCH | CtmFileWatcherSensor | FULL |
| ✓ Job:Database:EmbeddedQuery / :SQLScript / :StoredProcedure | CtmDatabaseJob (SQLExecuteQueryOperator) | FULL/PARTIAL |
| ✓ Job:Database:SSIS, :MSSQLAgent | not yet distinct — currently DATABASE row or MANUAL | PENDING |

## Class 2 — PROVIDER-MAPPABLE (bounded engineering; implement lazily, driven by real inventory)

Airflow has first-party/provider operators for most of these; each becomes a registry row
(+ optional thin Ctm* wrapper only if connectivity translation earns it — v4 policy).

| Family | Types | Airflow target |
|---|---|---|
| Big data (11) | ✓ Job:Hadoop:{Spark:Python, Spark:ScalaJava, Pig, Sqoop, Hive, DistCp, HDFSCommands, HDFSFileWatcher, Oozie, MapReduce, MapredStreaming} | apache-airflow-providers-apache-{spark,hive,hdfs}, Livy, etc. |
| Cloud DW / analytics (17) | ✓ Job:{Databricks, Snowflake, Snowflake Cortex AI, DBT, DataAssurance}; ✓ Job:AWS {Athena, Data Pipeline, DynamoDB, EMR, Redshift}; ✓ Job:Azure {Databricks, HDInsight, Synapse, AI Foundry}; ✓ Job:GCP {BigQuery, DataFlow, Dataproc}; ✓ Job:OCI Data Flow | providers: databricks, snowflake, dbt-cloud, amazon, microsoft-azure, google |
| Cloud compute (15) | ✓ Job:AWS {Batch, EC2, Lambda}; ✓ Job:Azure {App Services WebJobs, Batch Accounts, Functions(AzureFunctions), VM, VM Scale Sets}; ✓ Job:GCP {Batch, Eventarc, Functions, VM}; ✓ Job:OCI {Functions, VM}; ✓ Job:VMware By Broadcom (+ ✓ legacy Job:VMware:{Snapshot, Power, Configuration}) | providers: amazon, microsoft-azure, google; VMware → ssh/API |
| Containers (5) | ○ Amazon ECS, AWS App Runner, Azure Container Instances, GCP Cloud Run, Kubernetes | KubernetesPodOperator, EcsRunTaskOperator, CloudRun operators |
| CI/CD (5) | ○ Bitbucket, Azure DevOps, CircleCI, GitHub Actions, Jenkins | HTTP/provider operators (jenkins provider exists) |
| IaC (5) | ○ Ansible AWX, CloudFormation, Azure Resource Manager, GCP Deployment Manager, Terraform | HTTP/amazon/google providers; Terraform → BashOperator/HTTP |
| ETL / integration (26) | ✓ Job:Informatica, ✓ Job:IBMDataStage; ○ Airbyte, Alteryx Trifacta, Amazon RDS, Apache NiFi, AWS {AppFlow, DMS, Glue, Glue DataBrew}, Azure Data Factory, Boomi, Dataiku, Fivetran, GCP {Data Fusion, Dataplex, Dataprep}, Informatica CS, Matillion, Microsoft Fabric, OCI {Data Integration, Data Transforms}, Oracle Fusion ESS, SAP Integration Suite, Talend (+OAuth) | providers exist for Glue/ADF/DMS/AppFlow/Dataprep/Data Fusion; rest HTTP/API |
| BI (6) | ○ QuickSight, Power Automate, Power BI (+SP), Qlik Cloud, Tableau | providers: tableau, amazon; rest HTTP |
| Messaging / pub-sub (8) | ✓ Job:Messaging:{FreeText, WaitForReply, PreDefined} (JMS/MQ); ○ SNS, SQS, Kafka (Confluent), Azure Service Bus, RabbitMQ | providers: amazon (SNS/SQS), apache-kafka; JMS/MQ → custom |
| ML / AI (7) | ○ Bedrock, SageMaker, Azure ML, CrewAI, GCP Vertex AI, LangGraph, OCI Data Science | providers: amazon, google; CrewAI/LangGraph → HTTP/manual |
| Backup (6) | ✓ Job:NetBackup; ○ AWS Backup, AWS DataSync, Azure Backup, Rubrik, Veeam | amazon provider; rest HTTP/manual |
| Web services (3) | ✓ Job:WebServices; ○ REST, SOAP | HttpOperator |
| Workflow orchestrators (2 of 7) | ✓ Job:AWSStepFunction, ✓ Job:AzureLogicApps | providers: amazon, microsoft-azure |
| Java (1) | ✓ Job:Java | manual/custom (app-server specific) |

## Class 3 — AGENT-JUDGMENT domains (direction is incomplete; v7 decision layer)

| Family | Types | Why judgment |
|---|---|---|
| File transfer | ✓ Job:FileTransfer (5 modes, S3/Azure/GCS/AS2/SharePoint; MFT is a separate product line) | endpoints, direction, partner semantics per job |
| SAP (12) | ✓ Job:SAP:R3:{CREATE, PredefinedSapJob, MonitorSapJob, BatchInputSession, SapProfile:Activate/Deactivate, TriggerSapEvent, WatchSapEvent}; ✓ Job:SAP:BW:{ProcessChain, InfoPackage}; ✓ Job:SAP:DataArchiving:{Write, Delete, Store}; ○ SAP {BTP Scheduler, Datasphere, IBP} | some map (BW chain → API call), several have no Airflow analogue |
| Mainframe / midrange (15) | ✓ Job:zOS:{Member, InStreamJCL}; ✓ Job:OS400 (8 sub-modes); ✓ Job:Tandem:{TACLScript, Program, Command, EmbeddedTACLScript, ExternalProcess}; ○ Micro Focus, AWS Mainframe Modernization | usually replatform decisions, not conversions |
| ERP legacy | ✓ Job:PeopleSoft, ✓ Job:OEBS, ✓ Job:IBMCognos | per-process judgment |
| Custom | ✓ Job:ApplicationIntegrator:&lt;anything&gt; | unbounded by design — pure agent territory |

## Class 4 — ELIMINATE THE MIDDLEMAN (orchestrator job types)

○ Apache Airflow, Amazon MWAA, Astronomer, GCP Composer, GCP Workflows
([Application Workflows](https://documents.bmc.com/supportu/controlm-saas/en-US/Documentation/Jobs_for_Application_Workflows.htm)).
A Control-M job whose purpose is to trigger an Airflow DAG becomes a **native
dependency** (Asset/TriggerDagRun) in the target — the job itself disappears.

## Class 5 — INTERNAL / SLA

✓ Job:SLAManagement (△ BIM predecessor): eliminate or map to Airflow-native
monitoring/deadline alerts — decision per instance.

## Class 6 — CTM UTILITY JOBS (OS jobs running Control-M's own utilities; detect by command sniffing)

| Utility | Disposition | Airflow meaning |
|---|---|---|
| ctmorder / ctmorder -FORCE | **translate** | TriggerDagRunOperator |
| ctmvar | **translate** | Airflow Variable set (or DAG param) |
| ctmshout | **translate** | notification callback / EmailOperator |
| ctmcontb -ADD | **translate** | Asset event (outlet) |
| ctmcontb -DELETE/-CLEAN | **eliminate** (usually) | Airflow doesn't accumulate a condition table; only meaningful if masking a manual reset (→ Variable/XCom clear) |
| ctmfw | **translate** | file sensor (already a rules row when TASKTYPE) |
| ctmcreate | **translate** (rare) | TriggerDagRun / dynamic task |
| ctmstvar | **eliminate** | debug print |
| ctmldnrs (New Day manual conditions) | **eliminate** | no New Day concept |
| ctmudly (user dailies) | **eliminate** | schedules replace user dailies |
| ctmagcln, ctmruninf, ctmlog | **eliminate** | CTM metadata/log housekeeping; MWAA/CloudWatch handles it |
| ctmpsm, ctmkilljob | **ops-console** | operational actions — never DAG code; flag for runbook |
| start_ctm/shut_ctm, start-ag/shut-ag | **eliminate** | CTM infra lifecycle ("ctmstop/ctmstart" as such don't exist — site wrappers) |
| ctmdefine | **flag** | runtime self-modifying job defs — no static-DAG analogue; MANUAL |
| ctmhostgrp, ecactltb, ecaqrtab, ctmloadset | **eliminate/config** | host-group & resource-table admin → connections/pools config, not tasks |
| site `ctm*` wrapper scripts | **agent-judgment** | unknown wrapper → decision point, never silent |

## Unverified (referenced, no source confirmed this session)

Legacy Hadoop: Impala, Distributed Shell, generic Streaming, Tajo (2 types); literal
`Job:Airflow` string; classic pre-AAPI XML TASKTYPE enum beyond {Job, Command, Dummy,
Detached△}; `ctmrunon`, `ctmpwd` utility spellings.
