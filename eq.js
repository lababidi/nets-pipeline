
//TODO - get these two  from a configuration file.  Hard coded here.

function eventIndex() { return "nets-event"}
function eventDocType() { return "event"}

function eventUrl( fieldlist )
{
    var url =  eventIndex() + "/" + eventDocType()  + "/_search";
    if (fieldlist.length > 0)
        url = url + "?_source=" + fieldlist;
    return url
}

function query( query_string, only_search_title, start_date, end_date, places, people, organizations, offset, size )
{
    var payload = {
        "from": offset,
        "size": size,
        "query": {
            "filtered": {
                "query": {
                    "query_string": {
                        "query": query_string,
                        "analyze_wildcard": true,
                        "fields": only_search_title? ['title']: ['title', 'content']
                    }
                },
                "filter": [
                    {
                        "range": {
                            "date_published": {
                                "gte": start_date,
                                "le": end_date
                            }
                        }
                    }
                ]
            }
        }
    };

    if ( places.length  > 0 ) {
        for (var place in places)
            payload.query.filtered.filter.push( { "term" : { "places" : places[place] }})
    }
    if ( people.length  > 0 ) {
        for (var person in people)
            payload.query.filtered.filter.push( { "term" : { "people" : people[person] }})
    }
    if ( organizations.length  > 0 ) {
        for (var org in organizations)
            payload.query.filtered.filter.push( { "term" : { "organizations" : organizations[org] }})
    }

    return payload
}

function significant_terms( query_string, only_search_title, start_date, end_date, places, people, organizations)
{
    var payload = query( query_string, only_search_title, start_date, end_date, places, people, organizations, 0, 0);
    payload['size'] = 0;
    payload['aggs'] =   {
        "significantPlaces" : { "significant_terms" : { "field" : "places" } },
        "significantPeople" : { "significant_terms" : { "field" : "people" } }
    };

    return payload
}

function aggregates( query_string, only_search_title, start_date, end_date, places, people, organizations)
{
    var payload = query(query_string, only_search_title, start_date, end_date, places, people, organizations, 0, 0 );
    payload['size'] = 0;
    payload['aggs'] = {
        "places" : { "terms" : { "field" : "places" }},
        "people" : { "terms" : { "field" : "people" }},
        "organizations" : { "terms" : { "field" : "organizations" }}
    };

    return payload
}

