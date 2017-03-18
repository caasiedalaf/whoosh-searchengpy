from whoosh.qparser import QueryParser
from whoosh.qparser import MultifieldParser
from indexer import index
from whoosh.index import open_dir, exists_in
from whoosh.filedb.filestore import FileStorage
import os.path

def search(indices):
    s = '''SEARCH MODES:
       1. Search by category
       2. Search all categories
       Press ENTER to use default [Search by category]
<> '''
    inp = ''
    inpval = 0
    while True:
      inp = input(s)
      try:
        if inp.isspace() or ( len(inp) == 0):
          inpval = 1
          break
        inpval = int(inp)
        break;
      except:
       pass
    qparsers = []
    if inpval == 1:
       print('[SEARCH MODE] Choose category')
       s = '''\nThe following categories are available\n'''
       categories = []
       for ix in indices: 
        cat = list(ix.schema._fields.keys())
        for val in cat:
          if val in categories:
            val = val + " - " + ix.indexname
          categories.append(val)
       for i in range(1, len(categories) +  1):
         s += "\t" + str(i) + " to search in [" + categories[i-1] + "]  \n"
       s += "<> "
       while True:
         inp = input(s)
         try:
          inpval = int(inp)
          if inpval > i or inpval <= 0:
            raise Error()
          break;
         except:
          pass
       for i in range(0, len(indices)): 
        qparsers.append(QueryParser(categories[inpval - 1], indices[i].schema))
    else:
       print('[SEARCH MODE] All categories')
       categories = []
       for ix in indices:
        categories.extend(list(ix.schema._fields.keys()))
        qparsers.append(MultifieldParser(categories, ix.schema))
    s ='''Please input a QUERY string to search for in the database
e.g. Jurassic, Jurassic AND Ceratosaurus OR Stegosaurus etc.

You can also append a <==> to the end of the string to specify amount of  results to display
e.g. Jurassic <==> 10 i.e. Provide top 10 result of searching 'Jurassic'

You can press /QUIT\n
<> '''
    inp = input(s)
    while '/QUIT' not in inp.strip().upper():
       if (inp.strip() == ''):
         inp = input(s)
         continue
       data = inp.split('<==>')
       queries = [qparsers[i].parse(data[0].strip()) for i in range(0, len(qparsers))]
       limits = 10
       if len(data) > 1:
        try:
         limits = int(data[1].strip())
        except:
         limits = 10
       results = []
       stats = {}
       for i in range(0, len(indices)):
          searcher = indices[i].searcher()
          res = searcher.search(queries[i], limit=None)
          if len(res) != 0:
            stats[indices[i].indexname] = len(res)
          else:
           continue
          results.extend(res)
       if not results:
         inp = input(s)
         continue
       total_results = len(results)
       count = 0
       new_count = 0
       print("\n" + str(total_results) + " TOTAL hits\n", end="")
       if total_results == 0:
         inp = input(s)
         continue
       [print("\t\t" + str(stats[stat]) + ' found in: ' + str(stat)) for stat in stats]
       while count < total_results:
        new_count = count + limits
        if(new_count > total_results):
          new_count = total_results
       	print('\n\t\t\tDisplaying ' +  str(count + 1) + ' - ' + str(new_count) + ' of ' + str(total_results) + ' results.')
        for x in range(count, new_count):
          print( str(x + 1) + '. ' + str(results[x]) )
        opt = input('PRESS ANY KEY TO CONTINUE or SAVE to save results or DISCARD to discard results\n<> ')
        if 'SAVE' in opt.strip().upper():
          fname = 'SAMPLE_OUTPUT.RES'
          with open(fname, mode='w', encoding='utf-8') as a_file:
            [a_file.write("\t\t" + str(stats[stat]) + ' found in: ' + str(stat) + "\n") for stat in stats]
            a_file.write("\n")
            for row in range(0, len(results)):
             a_file.write( str(row + 1) + '. ' + str(tuple(results[row].values())) + "\n" )
          print('Results written to: ' + os.path.realpath(fname) + "\n")
          exit()
        if 'DISCARD' in opt.strip().upper():
          break   
        count = new_count  
       inp = input(s)
if '__main__' in __name__:
  indexdir =  "indexdir"
  storage = FileStorage(indexdir)
  fnames = storage.list()
  indices = []
  for f in fnames:
    if not f.endswith('.seg'):
     continue
    ind = f.split('_')[0]
    if exists_in(indexdir, indexname = ind):
     indices.append(open_dir(indexdir, ind))
  print(str(len(indices)) + ' searchables found...')
  #inp = input('ENTER database you will like to search: ')
  #while not exists_in(indexdir, indexname=inp.strip() ):
  #  print("\tDatabase: " + inp + " doesn't exist or hasn't been indexed at the moment")
  #  inp = input('ENTER database you will like to search: ')
  #ix = open_dir(indexdir, inp)
  search(indices)
