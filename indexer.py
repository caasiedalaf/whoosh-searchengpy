import sqlite3
import os
from whoosh.index import create_in
from whoosh.fields import *
from whoosh.index import open_dir
from whoosh.index import EmptyIndexError

def db_close(conn):
   try:
     conn.close()
   except:
     pass
     
def db_connect(dbname, *tables):
  if dbname == '':
       inp = input("Please specify a database name or press /N or /n to exit\n")
       if inp.startswith('/n') or inp.startswith('/N'):
         exit()
       else:
         dbname = inp
  conn = sqlite3.connect(dbname)
  conn.row_factory = sqlite3.Row
  if len(tables) == 0:
     tables = []
     try:
       df = conn.execute('SELECT name FROM sqlite_master WHERE type = "table";')
       data = df.fetchall()
       for row in data:
         for member in row:
           tables.append(member)
     except Exception as e:
       print("Error: " + str(e))
       inp = input("Continue? Yes or no\n")
       if 'y' in inp or 'Y' in inp:
         pass
       else:
         exit()
  return tables, conn, dbname

def index(dbnames_schemas = {}, indexdir = 'indexdir'):
  if len(dbnames_schemas) == 0:
    tables, conn, dbname = db_connect('')
    schema = ''
    parse_db(tables, conn, indexname=dbname, schema='', indexdir = indexdir)
  else:
   for i in dbnames_schemas.keys():
    tables, conn, dbname = db_connect(i)
    parse_db(tables, conn, indexname=i, schema= dbnames_schemas[i], indexdir=indexdir)
    
def parse_db(tables, conn, indexname, schema, indexdir):
  try:
   ix = open_dir(indexdir, indexname)
   print('\nIndex: ' + indexname + ' already exists in directory: ' + indexdir + ' Overwrite?. YES or NO')
   inp = input("<> ")
   if 'y' in inp or 'Y' in inp:
     pass
   else:
     return
  except:
   pass
  print('\n\nDATABASE: ' + indexname + ' contains ' + str(len(tables)) + ' tables')  
  for table in tables:
    print('Indexing table: ' + table + '...')
    t = tuple(table)
    c = conn.cursor()
    c.execute('SELECT * FROM ' + table + ";")
    rows = c.fetchall()
    if len(rows) != 0:
      keys = rows[0].keys()
      while ('Schema' not in str(type(schema))):
        if 'str' in str(type(schema)):
         if not schema.strip() is '':
           break
        inp = input("> You didn't provide a schema. Provide one? Yes or no (Use default schema): ")
        s = 'Schema('
        for key in keys:
           s += key.replace('\n', '').replace('\r', '').replace('\t', '').replace(' ', '') + "=TEXT(stored=True), "
        s = s.rstrip(", ")
        s += ")"
        if inp.strip().startswith('y') or inp.strip().startswith('Y'):
         print('Your database contains the following column names:')
         [print(x + ", ", end='') for x in keys]
         print('''\n\nPlease use them to generate a schema of the form: "Schema(COLUMN_NAME=whoosh.field.type, ...)"
            e.g. ''' + s)
         schema = input('Please input a schema: ')
         while not schema:
           schema = input('Please input a schema: ')
         schema.strip()
        else:
         print('Proceeding to use default schema: ' + s)
         schema = s
        break
      schema = eval(schema)
      if not os.path.exists(indexdir):
        os.mkdir(indexdir)
      ix = create_in(indexdir, schema=schema, indexname=indexname)
      writer = ix.writer()
      docline = ''
      print('Indexing ' + indexname)
      for row in rows:
         docline = 'writer.add_document('
         for key in keys:
            val = row[key]
            if "'" in val: 
             val = escape(val)
            val = val.replace("\t", "").replace("\n", "").replace("\r", "")   
            docline += key.replace('\n', '').replace('\r', '').replace('\t', '').replace(' ', '') + "=u'" + val + "', "
         docline = docline.rstrip(", ")
         docline += ")"
         print('...', end='\r')
         try:
          eval(docline)
         except Exception as e:
          print ('Error at: ' + docline)
          writer.cancel()
          print('Cancelled indexing due to error: ' + str(e))
          return
      writer.commit()
      print('Total data indexed: ' + str(len(rows)) )
    else:
      print('Database contains no valid rows')

def escape(s, obj = "'"):
  ret = ''
  for x in s:
    if x == obj:
      ret += '\\'
    ret += x
  return ret
  
if __name__ in '__main__':
   ix = None
   indexdir = "indexdir"
   dbnames_schemas = {'dinosaur.db' : 'Schema(Name=TEXT(stored=True), Description=TEXT(stored=True), Era=TEXT(stored=True), Url=ID(stored=True), Image=ID(stored=True))',
    'mmorpg.db' : '', 'superfamicom.db' : ''}
   print('Creating new index: ' + indexdir)
   index(
      dbnames_schemas, indexdir
      )
