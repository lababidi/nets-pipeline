
//TODO - get these two  from a configuration file.  Hard coded here.

function eventindex() { return "nets-event"}
function eventdoctype() { return "event"}

function eventurl( fieldlist )
{
    url =  eventindex() + "/" + eventdoctype()  + "/_search"
    if (fieldlist.length > 0)
        url = url + "?_source=" + fieldlist
    return url
}
function query( keywords,start_date, end_date, places, people, organizations, offset, size )
{
    payload = {
        "from" : offset,
        "size" : size,
        "query" : {
            "filtered" : {
                "query" : {
                    "multi_match" : {
                        "query" : keywords,
                        "fields" : ["title", 'content']
                  }
                },
                "filter" : [
                { "range" : { "date_published" : {
                    "gte" : start_date,
                    "le" : end_date }
                    }}
                ]
             }
        }
    }

    if ( places.length  > 0 ) {
        for (place in places)
            payload.query.filtered.filter.push( { "term" : { "places" : places[place] }})
    }
    if ( people.length  > 0 ) {
        for (person in people)
            payload.query.filtered.filter.push( { "term" : { "person" : people[person] }})
    }
    if ( organizations.length  > 0 ) {
        for (org in organizations)
            payload.query.filtered.filter.push( { "term" : { "organizations" : organizations[org] }})
    }

    return payload
}

function significant_terms( keywords, start_date, end_date, places, people, organizations)
{
    payload = query(keywords, start_date, end_date, places, people, organizations, 0, 0)
    payload['size'] = 0
    payload['aggs'] =   {
        "significantPlaces" : { "significant_terms" : { "field" : "places" } },
        "significantPeople" : { "significant_terms" : { "field" : "people" } }
    }

    return payload
}

function aggregates( keywords, start_date, end_date, places, people, organizations)
{
    payload = query(keywords, start_date, end_date, places, people, organizations, 0,0 )
    payload['size'] = 0
    payload['aggs'] = {
        "places" : { "terms" : { "field" : "places" }},
        "people" : { "terms" : { "field" : "people" }},
        "organizations" : { "terms" : { "field" : "organizations" }},

    }

    return payload
}

