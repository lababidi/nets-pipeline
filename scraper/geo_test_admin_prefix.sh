#!/usr/bin/env bash

#  ---- create sm-articles
#  Parents don’t need to know anything about their children
curl -XPOST localhost:9200/sm/sm-article/1 -d'{
  "content": "iran and the usa are disagreeing on nuclear talks",
  "places": ["iran", "usa"],
  "places_admin_prefix": ["world.iran", "world.usa"]
}'


curl -XPOST localhost:9200/sm/sm-article/2 -d'{
  "content": "tehran blamed isis for attacks on the ataturk ariport",
  "places": ["tehran", "ataturk_airport"],
  "places_admin_prefix": ["world.iran.tehran", "world.turkey.istanbul.Şenlikköy.Besyol"]
}'

curl -XPOST localhost:9200/sm/sm-article/3 -d'{
  "content": "iran increased oil production",
  "places": ["iran"],
  "places_admin_prefix": ["world.iran"]
}'

# ---- sample queries
curl -XPOST localhost:9200/sm/_search -d '{
    "query": {
        "bool": {
            "must":{
              "query_string":{
                 "query":"*",
                 "analyze_wildcard":true,
                 "fields":[
                    "content"
                 ]
              }
            },
            "filter":{
                "bool": {
                    "should": [
                        {
                            "prefix" : {
                                "places_admin_prefix" : "world.turkey.istanbul"
                            }
                        },
                        {
                            "prefix" : {
                                "places_admin_prefix" : "world.usa"
                            }
                        }
                    ]
                }
            }
        }
    }
}'

# Depricated "filtered" query
#curl -XPOST localhost:9200/sm/_search -d '{
#    "query": {
#        "filtered": {
#           "query":{
#              "query_string":{
#                 "query":"*",
#                 "analyze_wildcard":true,
#                 "fields":[
#                    "content"
#                 ]
#              }
#           },
#            "filter":[
#                {
#                    "term" : {
#                        "places" : "iran"
#                    }
#                }
#            ]
#        }
#    }
#}'

#  --- all these work
#{"index":["sm"]}
#{"from":0,"size":20,"query":{"query_string":{"query":"*","analyze_wildcard":true,"fields":["content"]}}}
#
#{"index":["sm"]}
#{"from":0,"size":20,"query":{"filtered":{"query":{"query_string":{"query":"*","analyze_wildcard":true,"fields":["content"]}},"filter":[{"term":{"places":"iran"}}]}}}
#
#{"index":["sm"]}
#{"from":0,"size":20,"query":{"filtered":{"query":{"query_string":{"query":"*","analyze_wildcard":true,"fields":["content"]}},"filter":[{"prefix":{"places":"world"}}]}}}
#
#{"index":["sm"]}
#{"from":0,"size":20,"query":{"filtered":{"query":{"query_string":{"query":"*","analyze_wildcard":true,"fields":["content"]}},"filter":[{"prefix":{"places_admin_prefix":"world.iran"}}]}}}