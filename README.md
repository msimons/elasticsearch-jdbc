![Alt text](../../../elasticsearch-river-jdbc/raw/master/src/site/origami.png "Optional title")

Elasticsearch JDBC river
========================
[![wercker status](https://app.wercker.com/status/923fd9217a917db5b3c415e8b696b5c7/m "wercker status")](https://app.wercker.com/project/bykey/923fd9217a917db5b3c415e8b696b5c7)

The Java Database Connection (JDBC) `river <http://www.elasticsearch.org/guide/reference/river/>`_  allows to fetch data from JDBC sources for indexing into `Elasticsearch <http://www.elasticsearch.org>`_. 

It is implemented as an `Elasticsearch plugin <http://www.elasticsearch.org/guide/reference/modules/plugins.html>`_.

The relational data is internally transformed into structured JSON objects for the schema-less indexing model in Elasticsearch. 

This fork of the elasticsearch JDBC river is supporting the following river types:
- Oneshot
- Simple
- Table
-

Creating a JDBC river is easy::

    curl -XPUT 'localhost:9200/_river/my_jdbc_river/_meta' -d '{
        "type" : "jdbc",
        "jdbc" : {
            "driver" : "com.mysql.jdbc.Driver",
            "url" : "jdbc:mysql://localhost:3306/test",
            "user" : "",
            "password" : "",
            "sql" : "select * from orders"
        }
    }'

Installation
------------

Prerequisites::

  Elasticsearch 1.4.2+
  a JDBC driver jar of your database

| ES version   |     Plugin version      |  Release date |
|----------|:-------------:|------:|
| 1.4.2+ |  **2.0.23** | February 20, 2015 |


License
=======

Elasticsearch JDBC River Plugin

Copyright (C) 2012,2013 Jörg Prante

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.