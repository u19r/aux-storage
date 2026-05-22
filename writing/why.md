# Why

This can sound like a lose/lose idea. Why use the DynamoDB API on other storage engines? Why give up the operational simplicity of DynamoDB and take on operational burden? Why give up the millions of hours invested in SQL systems and the extra capabilities they provide? Should companies try to compete with AWS engineers for operations and on-call support?

Those are fair objections. For many teams the right answer is simple: use DynamoDB, Postgres, PlanetScale, Spanner, or another mature database and move on. These systems are successful because they are good at their jobs. This project is not a claim that they are bad. It is a bet that there is useful room between them.

## The landscape

DynamoDB at the data-plane has not changed substantially in the last decade, with the important exception of strong multi-region writes. AWS has generally preferred purpose-built databases over adding broad new capabilities to DynamoDB. That is a reasonable product strategy, but it means users often build ETL pipelines, learn more services, and accept more moving parts.

DynamoDB is locked into AWS (but see ExtendDB). Optionality to run workloads on prem, in other clouds, and closer to users has value on its own.

DynamoDB is not cheap at high volume. A write-heavy workload can sometimes save a large amount by moving to EC2, and sometimes more by moving off AWS entirely. This can be false economy for a startup, because people cost more than infrastructure. At scale, for a profitable company with the right team, the savings can become large enough to matter.

Uptime is a systems question, not just a component. Uptime calculations are about more than a single database uptime.

The DynamoDB API has an important feature: it is hard for a single query or group of queries to cause accidental performance problems. Queries have a bounded shape and a visible cost. They are easy to analyse, calculate, and audit. Query performance in development, small scale, and large scale is often more similar than a SQL-based equivalent. This reduction in operational complexity comes at the cost of pure server efficiency. But servers keep getting cheaper.

With AI-assisted development, a database needs to be in the loop for reliable and fast tests. Mock databases can work, but having the real database close at hand is usually more reliable and efficient.

Distributed databases are no longer for scale when 1PB of disk, 4TB of memory, and 192 cores fit in a 2U. They are for availability. Offering a high availability service today can mean multiple servers, across different racks, in multiple data centres, and potentially in different providers or control planes.

FoundationDB is a fast, open source, durable, distributed database with a strong track record. If you tilt your head enough, it is a multi-writer distributed RocksDB where you can tail the WAL to send data cross-region and into object storage for point-in-time-recovery backups.

There are developers at AI labs, Apple, Adobe, Snowflake, and other companies with real experience and success using FoundationDB.

SQL will remain dominant. It has too much investment, too many experts, and it is very good at its job.

The DynamoDB API will always be unsuitable for many tasks that are straightforward in SQL.

AWS as a culture is not the same culture of 2012. Public pricing is no longer as aggressive as it once was, and private pricing agreements require a company of substantial size.

Using AWS is also an operations challenge. Services can be different enough that service spread becomes an operational risk.

The limited public information suggests AWS internal pricing for DynamoDB is very affordable, but private discounting is not always substantial. Not having an easy alternative to DynamoDB gives AWS negotiation leverage.

AWS does not offer reserved RCU / WCU purchases in all regions. Pricing is higher in some regions, and only one-year commitments are available in some places, which can limit saving potential for high-volume users.

If you are using AWS and you don't have customers then DynamoDB is very affordable.

There is a spectrum of availability needs for different businesses and for different stages of growth. Startups may take risks that need to be mitigated later. A business can start with SQLite, upgrade to Postgres, upgrade again to PlanetScale, move to PlanetScale sharded databases, or move to GCP Spanner for multi-region. That is a valid path, but it can mean higher cost per unit of compute, more latency, table migrations, and query changes.

PlanetScale Metal is a great service and should be one of the default options for a new service today.

## The bet

There are additional capabilities that can be built around the DynamoDB API. DynamoDB users today do a lot of manual work around item management, projections, query iteration, and multi-table setups. Some of that work can be collapsed into higher-level APIs while still keeping a transition path back to AWS.

Building multiple backends for a DynamoDB API is not much more complex than building a single backend once the provider boundary exists.

Offering SQLite, in-memory storage, and DynamoDB not only as an API but also as a library and AWS SDK replacement can make unit tests and end-to-end tests much faster. The developer productivity gains may be meaningful.

Developers will continue costing more than hardware.

Most developers do not know how to design applications around DynamoDB, and many will never need to. But there is a small group who do, who enjoy the model, and who can be productive with it. For that group, a more accessible version of DynamoDB may let them spend minimally more time on operations and and build much faster.

The DynamoDB API can be extended with new index types to solve common query pattern frustrations. When self-hosted, these index types can be affordable while still keeping compute and memory costs bounded.

DynamoDB ETL is an under-explored area in open source. Unlocking easy ETL will mitigate many downsides of DynamoDB that cannot be solved by using new indexes. DynamoDB items are a natural source for an OLAP database.

For a developer experienced with DynamoDB, a product that offers the same API for SQLite, Postgres, and FoundationDB may provide a clean roadmap. If the developer needs stronger HA at small scale, it should be possible to offer that capability with modest operational burden if they accept a latency tradeoff.

In some workloads, a much smaller self-hosted FoundationDB deployment could replace a much larger DynamoDB bill. The exact ratio depends on workload shape, team skill, and operational tolerance. Testing so far shows a reasonable comparison is $1,000 of self-hosted DynamoDB spend runs the same workloads as $15,000 to $40,000 of AWS DynamoDB.

There are some features or needs that don't need to be met. The entire DynamoDB API does not need to be implemented byte-for-byte. Infinite scaling can be dropped. Multi-tenancy and security can be handled in different layers.

There are customers who want to exit AWS or are in the process, but DynamoDB is the limiting factor.

Good public training materials on FoundationDB operations are limited. Better training could make developers and operators confident much faster.

### The build

- In-process SQL and KV stores for rapid iteration.
- Postgres, FoundationDB as additional backend targets.
- Passthrough libraries to AWS DynamoDB to support compatibility and transition.
- Opt-in HA clustering built into the application.
- Traditional global tables for multi-region deployments.
- HA and global tables as a migration path for scaling.
- Easy ETL mode to DuckDB and DuckLake.
- API extensions for missing DynamoDB features: auditing, expanded response size, item-level rollbacks, n + 1 collapsing, item-level streams, and BM25 indexes.
- An operations manual.
