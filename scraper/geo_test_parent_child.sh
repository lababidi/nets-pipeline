#!/usr/bin/env bash
# this is a sample document to demonstrate parent child relationships

#  ---- create mapping for sm-geo
#  have to create mapping first
curl -XPUT localhost:9200/sm -d '{
  "mappings": {
    "sm-article": {},
    "sm-geo": {
      "_parent": {
        "type": "sm-article" 
      },
      "properties": {
        "name": {
          "type": "string"
        },
        "location": {
          "type": "geo_shape",
          "tree": "geohash",
          "precision": "1km"
        }
      }       
    }
  }
}'

#  ---- create sm-articles
#  Parents don’t need to know anything about their children
curl -XPOST localhost:9200/sm/sm-article/1 -d'{
  "content": "iran and the usa are disagreeing on nuclear talks",
  "places": ["iran", "usa"]
}'


curl -XPOST localhost:9200/sm/sm-article/2 -d'{
  "content": "tehran blamed isis for attacks on the ataturk ariport",
  "places": ["tehran", "ataturk_airport"]
}'


#  ---- create sm-geo entries for countries
#  - When indexing child documents, you must specify the ID of the associated parent document
#  - The parent id creates the link between the parent and the child, and it ensures that the child document
#  is stored on the same shard as the parent.
#  - If you want to change the parent value of a child document, it is not sufficient to just reindex or update the
#  child document—the new parent document may be on a different shard. Instead, you must first delete the old child, and
#  then index the new child.
curl -XPOST localhost:9200/sm/sm-geo/usa?parent=1 -d '{
	"name" : "usa",
    "location" : {
        "type": "Polygon",
        "coordinates": [
          [
            [
              -126.91406249999999,
              29.22889003019423
            ],
            [
              -126.91406249999999,
              47.754097979680026
            ],
            [
              -60.8203125,
              47.754097979680026
            ],
            [
              -60.8203125,
              29.22889003019423
            ],
            [
              -126.91406249999999,
              29.22889003019423
            ]
          ]
        ]
    }
}'

curl -XPOST localhost:9200/sm/sm-geo/iran?parent=1 -d '{
	"name" : "iran",
    "location" : {
        "type": "Polygon",
        "coordinates": [
          [
            [
              43.9453125,
              39.095962936305504
            ],
            [
              60.46875,
              36.5978891330702
            ],
            [
              62.22656249999999,
              26.43122806450644
            ],
            [
              43.9453125,
              39.095962936305504
            ]
          ]
        ]
    }
}'

curl -XPOST localhost:9200/sm/sm-geo/tehran?parent=2 -d '{
	"name" : "tehran",
    "location" : {
        "type": "Polygon",
        "coordinates": [
          [
            [
              51.67968749999999,
              37.020098201368114
            ],
            [
              55.1953125,
              36.1733569352216
            ],
            [
              52.734375,
              35.31736632923788
            ],
            [
              51.67968749999999,
              37.020098201368114
            ]
          ]
        ]
    }
}'

curl -XPOST localhost:9200/sm/sm-geo/ataturk_airport?parent=2 -d '{
	"name" : "ataturk_airport",
    "location" : {
        "type": "Point",
        "coordinates": [
          32.689969539642334,
          39.95338867708912
        ]
    }
}'


curl -XPOST localhost:9200/sm/sm-article/_search -d '{
  "query": {
    "has_child": {
      "type":       "sm-geo",
      "filter": {
        "geo_shape": {
            "location": {
                "shape": {
                    "type": "envelope",
                    "coordinates" : [[-180, 90], [0,20]]
                },
                "relation": "within"
            }
        }
      }
    }
  }
}'

# ---- sample queries

# Depricated "filtered" query
curl -XPOST localhost:9200/sm/sm-article/_search -d '{
    "query": {
        "filtered": {
           "query":{
              "query_string":{
                 "query":"nuclear",
                 "analyze_wildcard":true,
                 "fields":[
                    "content"
                 ]
              }
           },
           "filter":[
              {
                 "has_child": {
                    "type":"sm-geo",
                    "filter": {
                       "geo_shape":{
                          "location":{
                             "shape":{
                                "type":"envelope",
                                "coordinates":[ [ -180,90 ], [ 0, 20 ] ]
                             },
                             "relation":"within"
                          }
                       }
                    }
                 }
              }
           ]
        }
    }
}'

curl -XPOST localhost:9200/sm/sm-article/_search -d '{
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
           "filter":[
              {
                 "has_child": {
                    "type":"sm-geo",
                    "filter": {
                       "geo_shape":{
                          "location":{
                             "shape":{
                                "type":"envelope",
                                "coordinates":[ [ -180,90 ], [ 0, 20 ] ]
                             },
                             "relation":"within"
                          }
                       }
                    }
                 }
              }
           ]
        }
    }
}'