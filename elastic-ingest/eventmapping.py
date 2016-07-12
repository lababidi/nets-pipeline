

def eventmapping():
    return {
            "event": {
                "properties": {
                    "publisher": {
                        "type": "string"
                    },
                    "language": {
                        "type": "string"
                    },
                    "publisher": {
                        "type": "string"
                    },
                    "content": {
                        "type": "string",
                        "index" : "analyzed"
                    },
                    "url": {
                        "type": "string"
                    },
                    "article": {
                        "type": "string"
                    },
                    "title": {
                        "type": "string"
                    },
                    "date_collected": {
                        "type": "date"
                    },
                    "date_published": {
                        "type": "string"
                    },
                    "date_collected_as_date_publshed": {
                        "type": "boolean"
                    },
                    "people": { "type" : "nested" },
                    "places": { "type" : "nested" },
                    "dates": { "type" : "nested" },
                    "times": { "type" : "nested" },
                    "organizations": { "type" : "nested" },
                    "languages": { "type" : "nested" },
                    "events": { "type" : "nested" },
                    "other": { "type" : "nested" }
                }
            }
        }
