

def eventmapping():
    return {
            "event": {
                "properties": {
                    "language":  { "index" : "not_analyzed", "type": "string"},
                    "publisher": {"type": "string"},
                    "content": {"type": "string", "index" : "analyzed"},
                    "url": {  "index" : "not_analyzed", "type": "string"},
                    "articleid": { "index" : "not_analyzed", "type": "string"},
                    "title": { "type": "string"},
                    "date_collected": {"type": "date"},
                    "date_published": {"type": "date"},
                    "date_collected_as_date_publshed": {"type": "boolean"},

                    "people": { "index" : "not_analyzed", "type" : "string" },
                    "places": { "index" : "not_analyzed", "type" : "string" },
                    "dates": { "index" : "not_analyzed", "type" : "string" },
                    "times": { "index" : "not_analyzed", "type" : "string" },
                    "organizations": { "index" : "not_analyzed","type" : "string" },
                    "languages": { "index" : "not_analyzed","type" : "string" },
                    "events": { "index" : "not_analyzed", "type" : "string" },
                    "other": { "index" : "not_analyzed", "type" : "string" }
                }
            }
        }
