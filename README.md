# Snowflake Elasticsearch Connector
![demo](screenshot/connector.gif)

A small utility to pull data from Snowflake and push it to Elasticsearch. 

[Snowflake](https://snowflake.com/) is a fully managed SaaS (software as a service) that provides a single platform for data warehousing, data lakes, data engineering, data science, data application development, and secure sharing and consumption of real-time / shared data. 

[Elasticsearch](https://www.elastic.co/elasticsearch/) is a search engine based on the Lucene library. It provides a distributed, multitenant-capable full-text search engine with an HTTP web interface and schema-free JSON documents.

If you want to get started with full text search capabilities on the top of the data which is stored on the snowflake, Feel free to use this connector which will pull data from Snowflake and push it to Elasticsearch.

# Installation

```sh
git clone https://github.com/ashishtiwari1993/snowflake-elasticsearch-connector.git
cd snowflake-elasticsearch-connector
```
### Installing dependencies

```sh
pip install -r requirements.txt
```

### Change configs

```
config/connector.yml
```

Add a credentials

```yaml
snowflake:
  username: sf_username
  password: sf_password
  account: sf_Organization-Account
  database: db_name
  table: table_name
  columns: ""
  warehouse: ""
  scheme: ""
  limit: 50

elasticsearch:
  host: https://localhost:9200
  username: elastic
  password: elastic@123
  ca_cert: /path/to/elasticsearch/config/certs/http_ca.crt 
  index: index_name
```
`limit` is the batch size of data. According to the above configuration, It will fetch 50 records at a time and push them to Elasticsearch. You can tune this according to your requirements.

# Run

```sh
python __main__.py
```

# Roadmap
- [ ] Snowflake
  - [ ] Add a query support
- [ ] Elasticsearch  
  - [ ] Elastic cloud support
  - [ ] Parallel push to Elasticsearch

# Contribute
Feel free to create a PR.
  
