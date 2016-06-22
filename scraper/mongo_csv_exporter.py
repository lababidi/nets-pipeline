
def export_mongo_csv(collection, query, fields, csv_filepath):
    csv_file = open(csv_filepath, 'w')
    query_results = collection.find(query)
    row_count = query_results.count()

    #write out headings first
    for f in fields:
        if f == fields[len(fields)-1]:
            csv_file.write(f+'\n')
        else:
            csv_file.write(f+', ')

    #then serialize the query results')
    for row in query_results:
      for f in fields:
        if f == fields[len(fields)-1]:
            if isinstance(row[f], basestring):
                txt = row[f]+'\n'
                txt = txt.encode('utf-8')
                csv_file.write(txt)
            else:
                csv_file.write(str(row[f])+'\n')
        else:
            if isinstance(row[f], basestring):
                txt = row[f]+', '
                txt = txt.encode('utf-8')
                csv_file.write(txt)
            else:
                csv_file.write(str(row[f])+', ')

    print('Exported '+str(row_count)+' entries to '+csv_filepath)
    csv_file.close()


