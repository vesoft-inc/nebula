
/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

namespace nebula {

const char* queryResultBody = R"(
{
    "took":2,
    "timed_out":false,
    "_shards":{
        "total":3,
        "successful":3,
        "skipped":0,
        "failed":0
    },
    "hits":{
        "total":{
            "value":11,
            "relation":"eq"
        },
        "max_score":1,
        "hits":[
            {
                "_index":"nebula_index_1",
                "_type":"_doc",
                "_id":"123",
                "_score":1,
                "_source":{
                    "vid":"1",
                    "text":"vertex text",
                    "src":"",
                    "dst":"",
                    "rank":0
                }
            },
            {
                "_index":"nebula_index_1",
                "_type":"_doc",
                "_id":"456",
                "_score":1,
                "_source":{
                    "vid":"",
                    "text":"edge text",
                    "src":"a",
                    "dst":"b",
                    "rank":10
                }
            }
        ]
    }
}
)";

}  // namespace nebula
